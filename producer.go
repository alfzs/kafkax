package kafkax

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Значения атрибута status у метрик продюсера.
const (
	statusSuccess = "success"
	statusError   = "error"
)

// Значения атрибута reason у kafkax.producer.messages.rejected. Множество
// замкнуто по построению: причина отбраковки — это ветка кода, а не входные
// данные, поэтому кардинальность метрики ограничена числом этих констант.
const (
	rejectEmptyTopic     = "empty_topic"
	rejectInvalidHeaders = "invalid_headers"
)

// PublishRequest — сообщение для отправки в Kafka.
//
// Все поля структуры доезжают до брокера: библиотека не съедает ни одного из
// них у себя. Тому, кому нужна тенантность в Kafka, нужен ключ или заголовок.
type PublishRequest struct {
	// Topic — топик назначения. Обязателен.
	Topic string
	// Key — ключ сообщения. Определяет партицию: записи с одинаковым ключом
	// попадают в одну партицию и потому упорядочены между собой.
	// nil означает партицию по кругу, а не «пустой ключ».
	Key []byte
	// Value — тело сообщения. nil — валидное значение: это tombstone,
	// который compacted-топик трактует как удаление ключа.
	Value []byte
	// Headers — пользовательские заголовки. Имена traceparent, tracestate и
	// baggage зарезервированы за OTel-propagator и отвергаются.
	Headers Headers
}

// MessageProducer — то, что умеет продюсер. Интерфейс объявлен здесь, чтобы
// вызывающий код мог подменить продюсер в тестах, не поднимая брокер.
type MessageProducer interface {
	SendMessage(ctx context.Context, req PublishRequest) error
	Close() error
}

// KafkaProducer — продюсер поверх *kgo.Client.
//
// Между вызовом и клиентом нет собственного слоя очередей и воркеров: он
// дублировал бы то, что клиент Kafka делает сам — батчинг, упорядочивание по
// партиции, ограничение памяти, — но с худшими свойствами, откладывая работу
// вместо того, чтобы её сокращать, и растягивая бюджет времени на отправку
// сверх документированного контракта.
//
// SendMessage вызывает ProduceSync, а батчингом, повторами и лимитом буфера
// занимается franz-go.
type KafkaProducer struct {
	client *kgo.Client
	logger *slog.Logger

	messageTimeout  time.Duration
	flushTimeout    time.Duration
	gracefulTimeout time.Duration

	// mu защищает closing и приём новых отправок в inflight. RWMutex, а не
	// atomic.Bool: между проверкой флага и inflight.Add должно быть
	// невозможно вклиниться Close'у, иначе Wait вернётся раньше отправки,
	// которая уже прошла проверку, и клиент закроется у неё под руками.
	mu       sync.RWMutex
	closing  bool
	inflight sync.WaitGroup

	sent     metric.Int64Counter
	failed   metric.Int64Counter
	rejected metric.Int64Counter
	duration metric.Float64Histogram

	// opts — готовые опции атрибутов для трёх инструментов исхода отправки.
	// Кэш с потолком, потому что топик приходит снаружи; см. optsCache.
	opts *optsCache
}

// Проверка на этапе компиляции, а не в тестах: интерфейс объявлен в этом же
// пакете, и рассинхрон с реализацией — опечатка, а не смена контракта.
var _ MessageProducer = (*KafkaProducer)(nil)

// NewKafkaProducer создаёт продюсер и подключается к брокерам лениво:
// franz-go не ходит в сеть при создании клиента, так что ошибка здесь —
// всегда ошибка конфигурации, а не доступности кластера.
func NewKafkaProducer(config Config) (*KafkaProducer, error) {
	// Не оборачивается: у агрегата валидации Unwrap() []error, и fmt.Errorf
	// подменил бы его на Unwrap() error — документированный разбор списка
	// перестал бы работать ровно там, где он нужен.
	if err := config.validateProducer(); err != nil {
		return nil, err
	}

	logger := config.logger("kafka_producer")

	opts, err := config.producerOpts(logger)
	if err != nil {
		return nil, fmt.Errorf("building producer options: %w", err)
	}

	tel := newTelemetry(config.ClientID, "")
	opts = append(opts, kgo.WithHooks(tel.hooks...))

	p := &KafkaProducer{
		logger:          logger,
		messageTimeout:  config.Producer.MessageTimeout,
		flushTimeout:    config.Producer.FlushTimeout,
		gracefulTimeout: config.GracefulTimeout,
	}

	if err := p.initMetrics(otel.GetMeterProvider().Meter(instrumentationName, meterOptions()...)); err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating kafka client: %w", err)
	}

	p.client = client

	return p, nil
}

// initMetrics регистрирует доменные метрики продюсера.
//
// Транспортные счётчики (соединения, байты, ошибки чтения/записи, записи и
// байты produce) приезжают из kotel и здесь не дублируются: он снимает их с
// хуков клиента, куда у этого слоя доступа нет. Именно счётчики: гистограмм в
// kotel v1.7.0 нет ни одной, латентность запроса к брокеру не измеряет никто —
// kafkax.producer.message.duration меряет SendMessage целиком, а не запрос.
//
// Счётчика kafkax.producer.panics здесь нет: собственных горутин у продюсера
// не заведено, восстанавливать паники негде и не из чего. Config.OnPanic
// вызывается только консьюмером.
func (p *KafkaProducer) initMetrics(meter metric.Meter) error {
	reg := &instrumentRegistry{}
	p.opts = newOptsCache(producerOptsLimit)

	sent, err := meter.Int64Counter("kafkax.producer.messages.sent",
		metric.WithDescription("Number of messages successfully delivered to Kafka"))
	p.sent = record(reg, "kafkax.producer.messages.sent", sent, err)

	failed, err := meter.Int64Counter("kafkax.producer.messages.failed",
		metric.WithDescription("Number of messages that failed to be delivered"))
	p.failed = record(reg, "kafkax.producer.messages.failed", failed, err)

	// Отбраковка на входе считается отдельно от отказов доставки, и атрибута
	// topic у неё нет намеренно: значение приходит снаружи и ничем не
	// ограничено, а серия рождается на каждое уникальное. Приложение,
	// подставляющее в топик пользовательский ввод, иначе роняло бы backend
	// метрик ровно теми запросами, которые пакет отверг не глядя.
	rejected, err := meter.Int64Counter("kafkax.producer.messages.rejected",
		metric.WithDescription("Messages rejected by input validation, by reason"))
	p.rejected = record(reg, "kafkax.producer.messages.rejected", rejected, err)

	// Единица — секунды, а не миллисекунды: при записи целыми миллисекундами
	// всё, что быстрее миллисекунды, попадало бы в гистограмму нулём — то есть
	// весь happy path при локальном брокере. Секунды к тому же требование OTel
	// к единицам длительности. Бакеты при этом обязаны быть свои: умолчание SDK
	// размечено под миллисекунды, см. producerDurationBuckets.
	duration, err := meter.Float64Histogram("kafkax.producer.message.duration",
		metric.WithDescription("End-to-end duration of SendMessage"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(producerDurationBuckets...))
	p.duration = record(reg, "kafkax.producer.message.duration", duration, err)

	return reg.err()
}

// SendMessage отправляет сообщение и ждёт подтверждения от брокера.
//
// Метод синхронный и потокобезопасный: параллельные вызовы батчатся внутри
// franz-go, поэтому «синхронный» здесь не означает «по одному запросу на
// сообщение».
//
// Бюджет времени один — Producer.MessageTimeout, отсчитывается от входа в
// метод и покрывает весь путь сообщения целиком, а не отдельные его этапы
// (постановка в очередь, ожидание результата, доставка), так что худший
// случай равен документированному значению, а не сумме нескольких таймеров.
//
// Дедлайн ctx этот бюджет только сокращает: срабатывает тот из двух, который
// раньше. Отмена ctx возвращает context.Canceled без сентинела пакета —
// отменил отправку сам вызывающий, а не Kafka.
func (p *KafkaProducer) SendMessage(ctx context.Context, req PublishRequest) (err error) {
	if !p.acquire() {
		return fmt.Errorf("send message: %w", ErrProducerClosed)
	}
	defer p.inflight.Done()

	// Валидация идёт до регистрации метрик исхода, а не после: атрибут topic
	// берётся из запроса и ничем не ограничен, так что писать его для
	// запроса, отвергнутого на входе, значит заводить три серии и полтора
	// десятка бакетов на каждое уникальное значение, пришедшее снаружи.
	// Отбраковка учитывается своим счётчиком с замкнутым множеством причин.
	if req.Topic == "" {
		p.reject(ctx, rejectEmptyTopic)

		return fmt.Errorf("send message: %w", ErrEmptyTopic)
	}

	if err := validateHeaders(req.Headers); err != nil {
		p.reject(ctx, rejectInvalidHeaders)

		return fmt.Errorf("send message: %w", err)
	}

	start := time.Now()

	// Единственная точка учёта исхода: и счётчики, и гистограмма заполняются
	// здесь, для любого результата похода в брокер.
	//
	// Гистограмма только успешных отправок систематически занижает хвост,
	// потому что таймауты — самые долгие вызовы — из неё выпадают.
	//
	// Контекст здесь пользовательский, а не sendCtx: этот defer зарегистрирован
	// раньше, чем defer cancel(), поэтому исполняется позже него: если контекст
	// отправки был собственным, к этому моменту он уже отменён. Экспортёр,
	// уважающий контекст, выбросил бы такую запись целиком.
	defer func() { p.recordSend(ctx, req.Topic, time.Since(start), err) }()

	// Дедлайн ставится и на контекст, и на запись (RecordDeliveryTimeout в
	// producerOpts) намеренно. Контекст отпускает вызывающего, но отменяет
	// батч только по контексту ПЕРВОЙ записи в нём, так что чужой батч может
	// пережить наш дедлайн; RecordDeliveryTimeout бьёт по каждой записи и
	// закрывает этот зазор.
	sendCtx, cancel := p.sendContext(ctx)
	defer cancel()

	rec := &kgo.Record{
		Topic:   req.Topic,
		Key:     req.Key,
		Value:   req.Value,
		Headers: toRecordHeaders(req.Headers),
	}

	// Спан publish целиком на kotel: он стартует его в OnProduceRecordBuffered
	// из rec.Context (который ProduceSync заполнит нашим ctx), инжектит
	// traceparent в заголовки записи и закрывает спан в
	// OnProduceRecordUnbuffered, проставив partition, offset и статус ошибки.
	if err := p.client.ProduceSync(sendCtx, rec).FirstErr(); err != nil {
		return p.produceError(req.Topic, err)
	}

	return nil
}

// sendContext возвращает контекст с бюджетом одной отправки.
//
// Свой context.WithTimeout не создаётся, когда у входного ctx дедлайн уже
// раньше Producer.MessageTimeout. Это не срезание угла: бюджет отправки по
// определению есть min(дедлайн вызывающего, now+MessageTimeout), и на этой
// ветке минимум — дедлайн вызывающего, то есть сам ctx.
//
// Так же поступает и стандартная библиотека: context.WithDeadlineCause при
// более раннем дедлайне родителя сводится к WithCancel(parent) и таймер не
// заводит вовсе. Остаётся сквозная обёртка, которая ничего не решает, — её мы
// и снимаем. Измерено на этой ветке: 2 alloc / 96 B на отправку против 0.
// Общий случай (у ctx дедлайна нет) стоит 4 alloc / 272 B и не меняется.
//
// Что при этом НЕ меняется — важнее того, что меняется, потому что это путь
// отмены отправки:
//
//   - Дедлайн у самой записи (kgo.RecordDeliveryTimeout в producerOpts) стоит
//     отдельно от контекста и этой веткой не затрагивается. Обещание «худший
//     случай равен MessageTimeout» держится им, а не обёрткой.
//   - Ошибка у вызывающего та же. На этой ветке его дедлайн срабатывает
//     первым и сегодня, а Err() обёртки — это Err() родителя, то есть
//     context.DeadlineExceeded по обе стороны правки; produceError переводит
//     его в ErrDeliveryTimeout одинаково.
//   - Уже истёкший ctx уходит по той же ветке и точно так же отказывает сразу:
//     WithTimeout от истёкшего родителя тоже возвращает отменённый контекст.
//
// Ноль в messageTimeout сделал бы ветку неэквивалентной (свой контекст истёк
// бы немедленно, а чужой — нет), но валидация конфигурации требует минимум 1s,
// см. producerErrors.
func (p *KafkaProducer) sendContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= p.messageTimeout {
		// Отменять нечего: контекст чужой. Пустая функция без захватов
		// компилируется в статическое значение и не аллоцирует, поэтому
		// возвращать её дешевле, чем заводить вторую сигнатуру ради ветки.
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, p.messageTimeout)
}

// recordSend записывает исход отправки: счётчик доставки и гистограмму
// длительности.
//
// Вынесено из тела defer в SendMessage отдельным методом, потому что это
// единственная точка учёта исхода на горячем пути: её цена измеряется
// бенчмарком, а измерить замыкание внутри defer нечем.
func (p *KafkaProducer) recordSend(ctx context.Context, topic string, elapsed time.Duration, err error) {
	// Два обращения к кэшу, а не одно: у счётчиков доставки атрибут только
	// topic — status у них был бы избыточен (само имя инструмента и есть
	// исход), а у гистограммы он нужен, иначе таймауты сливаются с успехами.
	topicOnly := p.opts.get(topic, noStatus)

	status := statusSuccess
	if err != nil {
		status = statusError

		p.failed.Add(ctx, 1, topicOnly.add...)
	} else {
		p.sent.Add(ctx, 1, topicOnly.add...)
	}

	p.duration.Record(ctx, elapsed.Seconds(), p.opts.get(topic, status).record...)
}

// reject считает сообщение, отвергнутое валидацией входа.
func (p *KafkaProducer) reject(ctx context.Context, reason string) {
	p.rejected.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// acquire регистрирует отправку, если продюсер ещё принимает сообщения.
func (p *KafkaProducer) acquire() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closing {
		return false
	}

	p.inflight.Add(1)

	return true
}

// produceError переводит ошибку franz-go в sentinel пакета, сохраняя причину.
//
// Разделение существует ради одного решения вызывающего кода: можно ли
// повторить отправку, не рискуя дубликатом. ErrDeliveryTimeout означает
// «запись уже у клиента и могла доехать», ErrProducerClosed — «не доехала
// точно».
//
// Причина разворачивается рядом с сентинелом во всех ветках, а не в двух из
// четырёх. Сентинел отвечает на вопрос «повторять ли», причина — на вопрос
// «что именно случилось»: наш context.DeadlineExceeded и kgo.ErrRecordTimeout
// приводят к одному и тому же ErrDeliveryTimeout, но означают разные проблемы
// (мал бюджет вызова против неспособности клиента дослать запись), и без
// причины они неразличимы.
func (p *KafkaProducer) produceError(topic string, err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, kgo.ErrRecordTimeout):
		return fmt.Errorf("send message: %w: %w", ErrDeliveryTimeout, err)

	case errors.Is(err, kgo.ErrClientClosed), errors.Is(err, kgo.ErrAborting):
		// Close успел закрыть клиент между acquire и ProduceSync либо клиент
		// сбрасывает буфер: с точки зрения вызывающего это тот же
		// «продюсер закрыт», что и проваленная проверка в acquire.
		return fmt.Errorf("send message: %w: %w", ErrProducerClosed, err)

	case errors.Is(err, context.Canceled):
		// Единственная ветка без сентинела: отмена — решение вызывающего, а не
		// отказ Kafka. Префикс называет операцию, а не причину: ctx.Done()
		// срабатывает и на отмене, и на дедлайне, и «context canceled: context
		// deadline exceeded» противоречило бы само себе.
		return fmt.Errorf("send message: %w", err)

	default:
		// Двойной %w: errors.Is находит sentinel, errors.As достаёт
		// *DeliveryError с кодом брокера, по которому и видно, имеет ли смысл
		// повтор.
		return fmt.Errorf("send message: %w: %w", ErrDeliveryFailed, newDeliveryError(topic, err))
	}
}

// Close останавливает приём новых сообщений, досылает буферизованные и
// закрывает клиент. Идемпотентен.
//
// Config.GracefulTimeout — общий бюджет на обе фазы, а не на каждую:
// Producer.FlushTimeout ограничивает сверху только вторую. Иначе закрытие
// продюсера могло бы занять GracefulTimeout + FlushTimeout, а вызывающий,
// который завёл общий бюджет на остановку приложения, ждёт одного числа.
func (p *KafkaProducer) Close() error {
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		p.logger.Warn("Kafka producer already in stopping state")

		return nil
	}

	p.closing = true
	p.mu.Unlock()

	p.logger.Info("Starting kafka producer shutdown")

	deadline := time.Now().Add(p.gracefulTimeout)

	p.awaitInflight(deadline)

	err := p.flush(deadline)

	// Close без бюджета: к этому моменту либо буфер пуст, либо ждать его
	// больше нечем — оставшиеся записи всё равно провалятся по промису.
	p.client.Close()
	p.logger.Info("Kafka producer shutdown completed")

	return err
}

// awaitInflight ждёт возврата из уже начатых SendMessage.
//
// Каждый такой вызов ограничен собственным MessageTimeout, так что ожидание
// конечно и без бюджета; бюджет здесь — защита от вызывающего, который
// передал в SendMessage контекст, живущий дольше, чем весь shutdown.
func (p *KafkaProducer) awaitInflight(deadline time.Time) {
	done := make(chan struct{})

	go func() {
		p.inflight.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Until(deadline)):
		p.logger.Warn("Timed out waiting for in-flight sends, proceeding to flush")
	}
}

// flush досылает буферизованные записи в пределах остатка бюджета.
//
// Оба отказа только возвращаются, без записи в лог: ошибку получает вызывающий
// Close, и он её залогирует — пакет, залогировав сам, удваивал бы событие в
// журнале. Число недосланных записей уходит в текст ошибки, а не в атрибут
// лога, ровно поэтому: иначе оно оставалось бы в строке, которую никто не
// связал бы с возвращённым ErrFlushIncomplete.
func (p *KafkaProducer) flush(deadline time.Time) error {
	budget := min(time.Until(deadline), p.flushTimeout)
	if budget <= 0 {
		return fmt.Errorf("closing producer: %w: flush budget exhausted, %d records buffered",
			ErrFlushIncomplete, p.client.BufferedProduceRecords())
	}

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	// kgo.Flush возвращает только ошибку, поэтому число недосланных сообщений
	// спрашивается отдельно — оно и есть то, что потеряется при закрытии
	// клиента.
	if err := p.client.Flush(ctx); err != nil {
		return fmt.Errorf("closing producer: %w: %d records remaining: %w",
			ErrFlushIncomplete, p.client.BufferedProduceRecords(), err)
	}

	p.logger.Info("All buffered messages flushed")

	return nil
}
