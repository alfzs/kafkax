package kafkax

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
)

// ПРАВИЛО ТЕКСТА ОШИБОК ПАКЕТА, единственное: обёртка через fmt.Errorf
// начинается с операции, названной герундием в нижнем регистре — «sending
// message: …», «closing producer: …», «creating kafka client: …».
//
// Одно правило вместо прежних пяти стилей сразу (от «start:» до
// «new_kafka_consumer:» в snake_case). Герундий выбран не жребием: он и так
// был самым частым здесь и совпадает с общепринятым в Go — текст читается как
// рассказ о том, что делалось, когда всё сломалось. Цена: префикс больше не
// совпадает с идентификатором метода буква в букву, и grep по «SendMessage»
// из лога промахнётся. Обратный вариант стоил бы дороже: у половины мест, где
// ошибка рождается, экспортированного метода над ними нет вовсе, а герундий
// одинаково описывает и внутренний шаг, и точку входа.
//
// Исключение одно — агрегат валидации конфигурации («kafkax: invalid producer
// config: …»). Это собственный текст типа configError, а не префикс обёртки, и
// он называет предмет претензии: «validating producer config» описывало бы
// сломанный валидатор вместо сломанного конфига.

// Sentinel-ошибки пакета. Проверяются через errors.Is; текст сообщения частью
// контракта не является и может меняться, в отличие от самих переменных.
//
// Разделение на «безопасно повторить» и «повтор даёт дубликат» — не украшение,
// а единственный способ для вызывающего кода отличить «сообщение не поставлено
// в очередь» от «сообщение, скорее всего, уже у брокера».
var (
	// ErrProducerClosed — Close уже вызван или идёт shutdown. Сообщение
	// не отправлено; повтор на этом продюсере бессмыслен.
	ErrProducerClosed = errors.New("kafkax: producer is shutting down")

	// ErrDeliveryTimeout — брокер не подтвердил доставку за MessageTimeout.
	// Сообщение уже отдано клиенту и МОГЛО быть доставлено: повтор способен
	// создать дубликат. Полагайтесь на идемпотентность потребителя или на
	// Producer.EnableIdempotence (включена по умолчанию).
	ErrDeliveryTimeout = errors.New("kafkax: timeout waiting for delivery ack")

	// ErrDeliveryFailed — брокер отверг сообщение. Оборачивает *DeliveryError:
	// используйте errors.As, чтобы прочитать код отказа и решить, имеет ли
	// смысл повтор (DeliveryError.Retriable).
	ErrDeliveryFailed = errors.New("kafkax: delivery failed")

	// ErrHandlerPanic — запаниковал чужой код на пути сообщения:
	// ConsumerHandler.ProcessMessage либо хук Config.OnMessageSkipped. Паника
	// перехватывается и превращается в обычную ошибку, чтобы сообщение прошло
	// штатный путь ретраев и коммита, а воркер партиции остался жив; паника
	// хука при этом означает отказ забрать сообщение, то есть отравление
	// партиции. Значение recover() развёрнуто в тексте ошибки, у хука — за
	// префиксом «on message skipped».
	ErrHandlerPanic = errors.New("kafkax: handler panic")

	// ErrConsumerClosed — консьюмер остановлен навсегда: Stop вызван или идёт
	// shutdown. Состояние терминальное, поэтому ошибку возвращают и Start, и
	// AddHandler: повторять их бессмысленно, нужен новый консьюмер.
	ErrConsumerClosed = errors.New("kafkax: consumer is shutting down")

	// ErrConsumerStarted — операция, разрешённая только до старта (Start,
	// AddHandler), вызвана на уже работающем консьюмере. В отличие от
	// ErrConsumerClosed говорит, что цикл опроса жив.
	ErrConsumerStarted = errors.New("kafkax: consumer already started")

	// ErrPollLoopStuck — цикл опроса не вышел даже после жёсткой отмены,
	// поэтому клиент Kafka намеренно НЕ закрыт: закрытие при живом цикле —
	// это гонка за картой воркеров и паника «send on closed channel», то есть
	// падение процесса вместо утечки одного клиента.
	//
	// Экземпляр утёк и не восстанавливается: партиции остаются закреплены за
	// ним до истечения session timeout после гибели процесса. Причина всегда
	// снаружи пакета — заблокировавшийся slog.Handler, экспортёр OTel или
	// обработчик, не реагирующий на отмену контекста.
	ErrPollLoopStuck = errors.New("kafkax: poll loop did not stop; kafka client left open")

	// ErrNoHandlers — ни одного обработчика не зарегистрировано через
	// AddHandler: подписываться и запускаться не на что.
	ErrNoHandlers = errors.New("kafkax: no handlers registered")

	// ErrEmptyTopic — топик не указан. Пустая строка отвергается на границе
	// API: иначе AddHandler зарегистрировал бы обработчик под пустым ключом,
	// а Start передал бы этот ключ в kgo.ConsumeTopics.
	ErrEmptyTopic = errors.New("kafkax: topic must not be empty")

	// ErrEmptyHeaderKey — заголовок с пустым именем. Поведение при чтении
	// зависит от клиента, поэтому такой заголовок не отправляется.
	ErrEmptyHeaderKey = errors.New("kafkax: header key must not be empty")

	// ErrReservedHeaderKey — заголовок с именем, которым управляет
	// OTel-propagator (traceparent/tracestate/baggage). Пропустить его значило
	// бы потерять пользовательские данные при перезаписи, поэтому такие
	// заголовки отвергаются на границе API.
	ErrReservedHeaderKey = errors.New("kafkax: header key is reserved for trace propagation")

	// ErrNilHandler — в AddHandler передан nil-обработчик. Без проверки nil
	// лёг бы в мапу, а паника случилась бы позже и в чужой горутине.
	ErrNilHandler = errors.New("kafkax: handler must not be nil")

	// ErrDuplicateHandler — обработчик для этого топика уже зарегистрирован.
	// Повторная регистрация — ошибка, а не тихая замена: два обработчика на
	// один топик почти всегда означают опечатку в имени, и молчаливая победа
	// последнего прячет её до продакшена.
	ErrDuplicateHandler = errors.New("kafkax: handler for topic already registered")

	// ErrFlushIncomplete — Close вернулся, не дождавшись отправки всех
	// буферизованных записей. Сообщения потеряны.
	//
	// Отдельный сентинел, потому что это единственная ошибка Close, на которую
	// обязана быть реакция: алерт, а не строчка в логе. Типовой сценарий —
	// `defer producer.Close()`, чья ошибка никем не читается; тогда о потере
	// не узнаёт никто.
	//
	// Оборачивает *FlushError: используйте errors.As, чтобы узнать, сколько
	// записей осталось в буфере.
	ErrFlushIncomplete = errors.New("kafkax: buffered records were not flushed")

	// ErrCommitFailed — финальный коммит отмеченных оффсетов не прошёл.
	//
	// Сообщения обработаны, но брокер об этом не узнал: после перезапуска они
	// приедут снова. Для at-least-once это штатная, а не аварийная ситуация —
	// но потребитель должен знать, что дубликаты будут, и отличать этот исход
	// от чистой остановки.
	ErrCommitFailed = errors.New("kafkax: final offset commit failed")

	// ErrInvalidConfig — конфигурация не прошла валидацию.
	//
	// Общий признак для всех ~25 проверок: errors.Is(err, ErrInvalidConfig)
	// отвечает на вопрос «это я неправильно настроил?», не требуя сверять
	// текст. Полный список конкретных претензий разворачивается через
	// errors.Unwrap() []error — сам сентинел в этот список не входит,
	// см. Config.Validate.
	ErrInvalidConfig = errors.New("kafkax: invalid configuration")
)

// DeliveryError — отказ доставки, описанный в терминах этого пакета, а не
// клиента Kafka.
//
// Заведён ради одной вещи: разбор ошибки отправки не должен требовать импорта
// github.com/twmb/franz-go/pkg/kerr. Зависимость от чужого типа ошибки ломается
// молча — смена клиента (один переход пакет уже пережил) не меняет ни одной
// сигнатуры, поэтому компилятор промолчит, а errors.As у потребителя просто
// перестанет находить, и «неповторяемый» отказ уедет в бесконечный ретрай.
//
// Всегда лежит под ErrDeliveryFailed:
//
//	var derr *kafkax.DeliveryError
//	if errors.As(err, &derr) && derr.Retriable {
//		// повтор осмыслен
//	}
//
// Исходная ошибка клиента остаётся достижимой через Unwrap, но частью контракта
// не является: смотреть на неё при отладке можно, полагаться в коде — нельзя.
type DeliveryError struct {
	// Topic — топик, в который шла отправка. В тексте ошибки он есть, но
	// разбирать текст ради него — ровно то, от чего избавляет типизация.
	Topic string
	// Code — код ошибки протокола Kafka. Ноль означает, что кода не было
	// вовсе: отказ случился до ответа брокера (сеть, TLS, разрешение имени).
	Code int16
	// Name — символическое имя кода протокола, например MESSAGE_TOO_LARGE.
	// Пусто при Code == 0.
	Name string
	// Description — расшифровка кода из спецификации протокола.
	Description string
	// Retriable — брокер считает отказ временным. При Code == 0 всегда false,
	// и это означает «клиент не сказал», а не «повторять бессмысленно».
	Retriable bool
	// Err — исходная ошибка клиента Kafka. Деталь реализации, см. Unwrap.
	Err error
}

func (e *DeliveryError) Error() string {
	if e.Code == 0 {
		return fmt.Sprintf("topic %q: %v", e.Topic, e.Err)
	}

	return fmt.Sprintf("topic %q: %s (code %d): %s", e.Topic, e.Name, e.Code, e.Description)
}

// Unwrap отдаёт ошибку клиента Kafka. Нужен для отладки и для того, чтобы
// errors.Is продолжал находить сентинелы самого franz-go в цепочке; строить на
// нём логику не следует — типы franz-go частью публичного контракта kafkax не
// объявлены.
func (e *DeliveryError) Unwrap() error { return e.Err }

// Is связывает тип с сентинелом: errors.Is(err, ErrDeliveryFailed) отвечает
// true и на *DeliveryError, полученной без обёртки.
func (e *DeliveryError) Is(target error) bool {
	return target == ErrDeliveryFailed
}

// newDeliveryError переводит отказ franz-go в тип пакета.
//
// Код и признак повторяемости достаются из *kerr.Error — единственного места,
// где эта информация вообще существует. Это и есть та точка, к которой
// сводится зависимость от клиента: при смене клиента правится она, а не код
// каждого потребителя.
func newDeliveryError(topic string, err error) *DeliveryError {
	de := &DeliveryError{Topic: topic, Err: err}

	if kerrErr, ok := errors.AsType[*kerr.Error](err); ok {
		de.Code = kerrErr.Code
		de.Name = kerrErr.Message
		de.Description = kerrErr.Description
		de.Retriable = kerrErr.Retriable
	}

	return de
}

// FlushError — Close не успел дослать буферизованные записи; Remaining из них
// потеряно безвозвратно.
//
// Число вынесено в поле, а не оставлено в тексте, по двум причинам. Первая:
// это величина, по которой принимают решение в коде, — потеря пяти сообщений
// и потеря пятидесяти тысяч требуют разной реакции, и счётчик потерь в метрике
// приложения строится только из числа, а не из строки. Вторая: пока число
// стояло в тексте, каждое его значение порождало отдельный шаблон сообщения, и
// APM группировал одну и ту же аварию в сотни разных.
//
// Цена названа: тот, кто просто печатает ошибку Close, числа больше не увидит —
// в тексте остались два стабильных исхода («flush budget exhausted» и причина
// от клиента). Чтобы вернуть число в лог, нужен errors.As:
//
//	if err := producer.Close(); err != nil {
//		var ferr *kafkax.FlushError
//		if errors.As(err, &ferr) {
//			slog.Error("kafka messages lost", "count", ferr.Remaining)
//		}
//	}
//
// Всегда лежит под ErrFlushIncomplete.
type FlushError struct {
	// Remaining — сколько записей осталось в буфере клиента на момент отказа.
	// Ноль возможен: буфер мог опустеть между отказом Flush и опросом клиента.
	Remaining int64
	// Err — причина отказа Flush. nil означает, что flush не начинался вовсе:
	// бюджет завершения кончился раньше.
	Err error
}

func (e *FlushError) Error() string {
	if e.Err == nil {
		return "flush budget exhausted"
	}

	return e.Err.Error()
}

// Unwrap отдаёт причину отказа Flush, чтобы errors.Is находил в цепочке
// context.DeadlineExceeded и сентинелы franz-go. При исчерпанном бюджете
// причины нет и цепочка на этом кончается.
func (e *FlushError) Unwrap() error { return e.Err }

// Is связывает тип с сентинелом: errors.Is(err, ErrFlushIncomplete) отвечает
// true и на *FlushError, полученной без обёртки.
func (e *FlushError) Is(target error) bool {
	return target == ErrFlushIncomplete
}
