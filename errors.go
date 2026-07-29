package kafkax

import "errors"

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

	// ErrDeliveryFailed — брокер отверг сообщение. Оборачивает конкретную
	// ошибку franz-go: используйте errors.As с *kerr.Error, чтобы прочитать
	// код и решить, имеет ли смысл повтор (kerr.MessageTooLarge — нет,
	// kerr.NotEnoughReplicas — да).
	ErrDeliveryFailed = errors.New("kafkax: delivery failed")

	// ErrHandlerPanic — ConsumerHandler.ProcessMessage запаниковал. Паника
	// перехватывается и превращается в обычную ошибку обработки, чтобы
	// сообщение прошло штатный путь ретраев и коммита, а воркер партиции
	// остался жив.
	ErrHandlerPanic = errors.New("kafkax: handler panic")

	// ErrConsumerClosed — Stop уже вызван или идёт shutdown.
	ErrConsumerClosed = errors.New("kafkax: consumer is shutting down")

	// ErrConsumerStarted — Start вызван повторно на уже запущенном консьюмере.
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
)
