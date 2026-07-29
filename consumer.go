package kafkax

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// IncomingMessage — сообщение Kafka, переданное в ConsumerHandler.
//
// Key, Value и Headers ссылаются на буферы записи franz-go и живут ровно
// столько, сколько длится вызов обработчика. Код, сохраняющий их дольше,
// обязан копировать.
type IncomingMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   Headers
	// Timestamp — временная метка записи (CreateTime или LogAppendTime, в
	// зависимости от настройки топика).
	Timestamp time.Time
}

// ConsumerHandler обрабатывает одно сообщение.
//
// Возвращённая ошибка означает «обработка не удалась»: сообщение пойдёт по
// пути повторов (Consumer.HandlerMaxRetries), а исчерпав их — попадёт в
// Config.OnMessageSkipped либо, если хук не задан, остановит свою партицию на
// непрокоммиченном оффсете. Подробно — в документации пакета, раздел «Политика
// повторов».
//
// Паника внутри ProcessMessage перехватывается и превращается в ошибку,
// оборачивающую ErrHandlerPanic, то есть идёт тем же путём, что и обычный
// отказ, а не роняет воркер.
//
// Обработчик обязан быть идемпотентным: гарантия пакета — at-least-once, и
// повторная обработка одного и того же сообщения после ребаланса или
// перезапуска штатна, а не исключительна.
//
// Возврат nil означает «сообщение обработано, оффсет можно двигать». Возвращать
// nil при неудаче, чтобы «не застревать», — это молчаливая потеря данных; для
// осознанного пропуска существует OnMessageSkipped.
type ConsumerHandler interface {
	ProcessMessage(ctx context.Context, msg IncomingMessage) error
}

// ConsumerHandlerFunc адаптирует функцию к ConsumerHandler.
type ConsumerHandlerFunc func(ctx context.Context, msg IncomingMessage) error

// ProcessMessage реализует ConsumerHandler.
func (f ConsumerHandlerFunc) ProcessMessage(ctx context.Context, msg IncomingMessage) error {
	return f(ctx, msg)
}

// MessageConsumer — контракт консьюмера.
//
// Отдельного шага подписки нет: набор топиков задаётся при создании клиента
// (kgo.ConsumeTopics), топики берутся из зарегистрированных обработчиков, а сам
// клиент создаётся внутри Start. Stop возвращает ошибку — иначе неудача
// финального коммита оффсетов оставалась бы видна только в логах.
type MessageConsumer interface {
	AddHandler(topic string, handler ConsumerHandler, mws ...ConsumerMiddleware) error
	Start(ctx context.Context) error
	Stop() error
}

var _ MessageConsumer = (*KafkaConsumer)(nil)

// Значения атрибута status у метрик kafkax.consumer.messages.processed и
// kafkax.consumer.message.duration.
const (
	// consumerStatusSuccess — обработчик вернул nil, запись отмечена к коммиту.
	consumerStatusSuccess = "success"
	// consumerStatusError — обработчик исчерпал повторы и не справился.
	// Запись НЕ отмечена: at-least-once держится именно на этом.
	consumerStatusError = "error"
	// consumerStatusSkipped — до вердикта обработчика дело не дошло: на топик
	// нет обработчика либо отмена контекста прервала паузу между повторами.
	consumerStatusSkipped = "skipped"
)

// workerKey — адрес партиционного воркера. Структура сравнима, поэтому годится
// ключом карты без конкатенации строк на горячем пути.
type workerKey struct {
	topic     string
	partition int32
}

// partitionWorker — очередь и горутина одной топик-партиции. Одна горутина на
// партицию даёт параллелизм между партициями и строгий порядок внутри.
type partitionWorker struct {
	// records — батчи из одного опроса. Буфер ёмкостью
	// Consumer.MessageQueueSize определяет, насколько цикл опроса может
	// обгонять обработку; при переполнении опрос блокируется, и это и есть
	// backpressure.
	records chan []*kgo.Record
	// done закрывается горутиной воркера при выходе.
	done chan struct{}
	// cancel обрывает обработку жёстко — когда мягкая остановка не уложилась
	// в бюджет и партицию пора отпустить, чем бы воркер ни был занят.
	cancel context.CancelFunc
	// stopOnce защищает records от повторного закрытия: остановить воркер
	// могут и колбэк ребаланса, и Stop.
	stopOnce sync.Once

	// poisoned означает, что запись в этой партиции не удалось ни обработать,
	// ни отдать в OnMessageSkipped, и её оффсет не отмечен.
	//
	// Флаг обязателен, а не декоративен: MarkCommitRecords отмечает оффсет, а
	// не сообщение, поэтому отметка любой ПОСЛЕДУЮЩЕЙ записи сдвинула бы
	// коммит за проваленную и потеряла бы её. Отказ отмечать одну запись
	// сохраняет at-least-once только вместе с отказом отмечать всё, что за
	// ней.
	//
	// Читается и пишется исключительно горутиной воркера, поэтому без
	// синхронизации.
	poisoned bool
}

// stop закрывает очередь; воркер дообработает буфер и выйдет сам.
func (w *partitionWorker) stop() {
	w.stopOnce.Do(func() { close(w.records) })
}

// consumerMetrics — собственные метрики домена консьюмера. Транспортный
// уровень (задержки запросов, размеры батчей, состояние соединений) измеряет
// kotel, дублировать его здесь нечем.
type consumerMetrics struct {
	processed     metric.Int64Counter
	duration      metric.Float64Histogram
	retries       metric.Int64Counter
	fetchErrors   metric.Int64Counter
	workersActive metric.Int64UpDownCounter
	panics        metric.Int64Counter
}

// newConsumerMetrics регистрирует инструменты, собирая все ошибки разом.
//
// Ни у одной метрики нет атрибута partition: он умножает кардинальность на
// число партиций, а диагностическая ценность нулевая — привязку к партиции
// даёт спан обработки, где она есть и без метрик.
func newConsumerMetrics(meter metric.Meter) (consumerMetrics, error) {
	var reg instrumentRegistry

	processedName := "kafkax.consumer.messages.processed"
	processed, err := meter.Int64Counter(processedName,
		metric.WithDescription("Messages that reached a terminal outcome, by topic and status"))
	m := consumerMetrics{processed: record(&reg, processedName, processed, err)}

	// Единица — секунды, а не миллисекунды: Milliseconds() усекает до целого,
	// и все длительности быстрее миллисекунды попадали бы в нулевую корзину.
	// Seconds() усечения не делает.
	durationName := "kafkax.consumer.message.duration"
	duration, err := meter.Float64Histogram(durationName,
		metric.WithDescription("End-to-end handler time including all retries and retry delays"),
		metric.WithUnit("s"))
	m.duration = record(&reg, durationName, duration, err)

	retriesName := "kafkax.consumer.handler.retries"
	retries, err := meter.Int64Counter(retriesName,
		metric.WithDescription("Handler invocations that failed and were retried"))
	m.retries = record(&reg, retriesName, retries, err)

	fetchErrorsName := "kafkax.consumer.fetch.errors"
	fetchErrors, err := meter.Int64Counter(fetchErrorsName,
		metric.WithDescription("Partition-level errors returned by a poll"))
	m.fetchErrors = record(&reg, fetchErrorsName, fetchErrors, err)

	workersName := "kafkax.consumer.workers.active"
	workers, err := meter.Int64UpDownCounter(workersName,
		metric.WithDescription("Partition workers currently running"))
	m.workersActive = record(&reg, workersName, workers, err)

	panicsName := "kafkax.consumer.panics"
	panicsCounter, err := meter.Int64Counter(panicsName,
		metric.WithDescription("Panics recovered inside kafkax consumer goroutines, by site"))
	m.panics = record(&reg, panicsName, panicsCounter, err)

	return m, reg.err()
}

// KafkaConsumer — консьюмер Kafka поверх franz-go.
//
// На каждую назначенную топик-партицию заводится горутина: обработка разных
// партиций идёт параллельно, внутри одной партиции — строго по порядку
// оффсетов. Методы безопасны для вызова из разных горутин.
type KafkaConsumer struct {
	config    Config
	logger    *slog.Logger
	telemetry telemetry
	metrics   consumerMetrics
	panics    panicReporter

	handlersMu sync.RWMutex
	handlers   map[string]ConsumerHandler

	// workers живёт без мьютекса, и это следствие kgo.BlockRebalanceOnPoll:
	// опрос и колбэки ребаланса становятся взаимно исключающими, так что
	// единственные, кто трогает карту, — цикл опроса и колбэки — никогда не
	// работают одновременно.
	workers map[workerKey]*partitionWorker

	// mu защищает только переходы жизненного цикла (client, pollCancel):
	// Start и Stop могут быть вызваны из разных горутин.
	mu         sync.Mutex
	client     *kgo.Client
	pollCancel context.CancelFunc

	// lifeCtx — жизненный цикл консьюмера: от него наследуются воркеры и цикл
	// опроса. Отмена означает жёсткую остановку без дренажа очередей.
	//
	// Контекст в поле — ровно тот случай, против которого containedctx не
	// возражает по существу: это область жизни объекта, а не контекст запроса.
	// Передавать его параметром некому — воркеры создаются колбэками ребаланса,
	// которые вызывает franz-go, и своего контекста у них нет.
	//
	//nolint:containedctx // область жизни консьюмера, а не контекст запроса
	lifeCtx    context.Context
	lifeCancel context.CancelFunc

	// loopDone закрывается циклом опроса при выходе; Stop ждёт именно его,
	// прежде чем трогать карту воркеров.
	loopDone chan struct{}

	started  atomic.Bool
	stopping atomic.Bool
}

// NewKafkaConsumer создаёт консьюмера.
//
// Соединения здесь не устанавливаются и горутины не запускаются: набор топиков
// известен только после AddHandler, а franz-go требует его при создании
// клиента, поэтому сам клиент создаётся в Start. Конструктор проверяет
// конфигурацию, готовит логгер, метрики и репортер паник.
func NewKafkaConsumer(config Config) (*KafkaConsumer, error) {
	const op = "new_kafka_consumer"

	if err := config.validateConsumer(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	logger := config.logger("kafka_consumer").With(slog.String("group", config.Consumer.Group))

	metrics, err := newConsumerMetrics(otel.Meter(instrumentationName))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	lifeCtx, lifeCancel := context.WithCancel(context.Background())

	return &KafkaConsumer{
		config:     config,
		logger:     logger,
		telemetry:  newTelemetry(config.ClientID, config.Consumer.Group),
		metrics:    metrics,
		panics:     panicReporter{logger: logger, panics: metrics.panics, onPanic: config.OnPanic},
		handlers:   make(map[string]ConsumerHandler),
		workers:    make(map[workerKey]*partitionWorker),
		lifeCtx:    lifeCtx,
		lifeCancel: lifeCancel,
		loopDone:   make(chan struct{}),
	}, nil
}

// AddHandler регистрирует обработчик топика и оборачивает его в mws.
//
// Вызывается до Start: после старта набор топиков уже передан в
// kgo.ConsumeTopics, и добавление обработчика вернуло бы обработчик без
// подписки. Повторная регистрация того же топика — ошибка, а не тихая замена.
func (c *KafkaConsumer) AddHandler(topic string, handler ConsumerHandler, mws ...ConsumerMiddleware) error {
	if topic == "" {
		return fmt.Errorf("add handler: %w", ErrEmptyTopic)
	}

	// Сравнение с nil ловит только нетипизированный nil; типизированный
	// nil-указатель в интерфейсе пройдёт, но его паника станет
	// ErrHandlerPanic — то есть штатной ошибкой обработки.
	if handler == nil {
		return fmt.Errorf("add handler for topic %q: %w", topic, ErrNilHandler)
	}

	if c.started.Load() {
		return fmt.Errorf("add handler for topic %q: %w", topic, ErrConsumerStarted)
	}

	c.handlersMu.Lock()
	defer c.handlersMu.Unlock()

	if _, exists := c.handlers[topic]; exists {
		return fmt.Errorf("handler for topic %q already registered", topic)
	}

	// Цепочка middleware собирается один раз при регистрации, а не на каждое
	// сообщение: аллокации замыканий на горячем пути ничего не дают.
	c.handlers[topic] = Chain(handler, mws...)

	return nil
}

// Start создаёт клиента Kafka и запускает цикл опроса. Не блокирует.
//
// Отмена ctx эквивалентна Stop, но без дренажа очередей и без финального
// коммита: предпочтительнее явный Stop. Повторный вызов возвращает
// ErrConsumerStarted; консьюмер, прошедший Stop, не перезапускается.
func (c *KafkaConsumer) Start(ctx context.Context) error {
	const op = "start"

	if !c.started.CompareAndSwap(false, true) {
		return ErrConsumerStarted
	}

	// Флаг сбрасывается на каждом неуспешном пути: иначе после отказа Start
	// исправить конфигурацию и повторить запуск было бы нельзя.
	topics := c.topics()
	if len(topics) == 0 {
		c.started.Store(false)

		return ErrNoHandlers
	}

	if c.stopping.Load() {
		c.started.Store(false)

		return ErrConsumerClosed
	}

	opts, err := c.config.consumerOpts(c.logger, topics, rebalanceCallbacks{
		assigned: c.onPartitionsAssigned,
		revoked:  c.onPartitionsRevoked,
		lost:     c.onPartitionsLost,
	})
	if err != nil {
		c.started.Store(false)

		return fmt.Errorf("%s: %w", op, err)
	}

	// Хуки kotel питают трейсер: именно OnFetchRecordBuffered кладёт в
	// rec.Context извлечённый из заголовков W3C trace context, на котором
	// потом строится спан обработки.
	opts = append(opts, kgo.WithHooks(c.telemetry.hooks...))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		c.started.Store(false)

		return fmt.Errorf("%s: kafka client init: %w", op, err)
	}

	pollCtx, pollCancel := context.WithCancel(c.lifeCtx)

	c.mu.Lock()
	c.client = client
	c.pollCancel = pollCancel
	c.mu.Unlock()

	go c.watchContext(ctx)
	go c.runPollLoop(pollCtx, client)

	c.logger.Info("Kafka consumer started", slog.Any("topics", topics))

	return nil
}

// watchContext переводит отмену пользовательского ctx в отмену жизненного
// цикла. Сам завершается вместе с ним, чтобы не пережить консьюмера.
func (c *KafkaConsumer) watchContext(ctx context.Context) {
	select {
	case <-ctx.Done():
		c.lifeCancel()
	case <-c.lifeCtx.Done():
	}
}

// topics возвращает отсортированный набор топиков с обработчиками.
// Сортировка нужна для воспроизводимости логов и сообщений об ошибках:
// порядок обхода карты в Go случаен.
func (c *KafkaConsumer) topics() []string {
	c.handlersMu.RLock()
	defer c.handlersMu.RUnlock()

	return slices.Sorted(maps.Keys(c.handlers))
}

func (c *KafkaConsumer) handler(topic string) (ConsumerHandler, bool) {
	c.handlersMu.RLock()
	defer c.handlersMu.RUnlock()

	h, ok := c.handlers[topic]

	return h, ok
}

// runPollLoop — единственный читатель клиента и единственный писатель в очереди
// воркеров.
func (c *KafkaConsumer) runPollLoop(ctx context.Context, client *kgo.Client) {
	defer close(c.loopDone)

	for {
		fetches := client.PollRecords(ctx, c.config.Consumer.MaxPollRecords)

		// Оба условия проверяются до разбора ошибок: закрытие клиента и отмена
		// контекста приезжают синтетическим фетчем с ошибкой в нулевой
		// партиции, и принимать их за отказ брокера не за чем.
		if fetches.IsClientClosed() {
			return
		}

		if err := fetches.Err0(); errors.Is(err, context.Canceled) {
			return
		}

		// Обход ошибок вынесен наружу цикла по записям намеренно: партиция с
		// фатальной ошибкой и без записей приезжает отдельным пустым фетчем и
		// изнутри обхода Records не видна вовсе.
		fetches.EachError(c.reportFetchError)

		fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
			c.dispatch(ctx, client, ftp)
		})

		// Обязательно на каждой итерации: без этого следующий ребаланс —
		// включая тот, что инициирует закрытие клиента, — повиснет навсегда.
		client.AllowRebalance()
	}
}

// reportFetchError фиксирует ошибку уровня партиции.
func (c *KafkaConsumer) reportFetchError(topic string, partition int32, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed) {
		return
	}

	ctx := context.WithoutCancel(c.lifeCtx)

	c.metrics.fetchErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", topic)))
	c.logger.Error("Partition fetch error",
		slog.String("topic", topic),
		slog.Int("partition", int(partition)),
		slog.Any("error", err))
}

// dispatch отдаёт батч воркеру партиции.
//
// Отправка блокирующая и без таймаута намеренно: батч, выброшенный из-за
// переполнения очереди, был бы перепрыгнут коммитом следующего, и сообщения
// потерялись бы молча. Блокировка тормозит опрос, а не теряет данные.
func (c *KafkaConsumer) dispatch(ctx context.Context, client *kgo.Client, ftp kgo.FetchTopicPartition) {
	if len(ftp.Records) == 0 {
		return
	}

	worker := c.worker(client, workerKey{topic: ftp.Topic, partition: ftp.Partition})

	select {
	case worker.records <- ftp.Records:
	// Воркер мог умереть (паника) или уйти по отмене: без этой ветки опрос
	// встал бы навсегда на партиции, которую некому читать.
	case <-worker.done:
	case <-ctx.Done():
	}
}

// worker возвращает воркера партиции, создавая его при необходимости.
//
// Обычно воркер уже создан колбэком назначения; ленивое создание закрывает
// окно, в котором фетч приезжает раньше колбэка, и стоит одну проверку карты.
// Мьютекса не требует: см. комментарий у поля workers.
func (c *KafkaConsumer) worker(client *kgo.Client, key workerKey) *partitionWorker {
	if w, ok := c.workers[key]; ok {
		return w
	}

	ctx, cancel := context.WithCancel(c.lifeCtx)
	w := &partitionWorker{
		records: make(chan []*kgo.Record, c.config.Consumer.MessageQueueSize),
		done:    make(chan struct{}),
		cancel:  cancel,
	}
	c.workers[key] = w

	c.metrics.workersActive.Add(ctx, 1)

	go c.runPartitionWorker(ctx, client, key, w)

	return w
}

// runPartitionWorker последовательно обрабатывает батчи одной партиции.
func (c *KafkaConsumer) runPartitionWorker(
	ctx context.Context, client *kgo.Client, key workerKey, w *partitionWorker,
) {
	logger := c.logger.With(slog.String("topic", key.topic), slog.Int("partition", int(key.partition)))

	// Порядок регистрации обратен порядку исполнения: сначала перехват паники,
	// потом освобождение ресурсов, и только затем сигнал о завершении.
	defer close(w.done)
	defer w.cancel()
	defer c.metrics.workersActive.Add(context.WithoutCancel(ctx), -1)
	defer func() {
		if r := recover(); r != nil {
			// Паника здесь — не в обработчике, а в самом воркере: без
			// перехвата она уронила бы процесс, потому что чужая горутина
			// вызывающим кодом не ловится.
			c.panics.report(context.WithoutCancel(ctx), panicSitePartitionWorker, r, debug.Stack(),
				slog.String("topic", key.topic),
				slog.Int("partition", int(key.partition)))
		}
	}()

	logger.Debug("Partition worker started")

	for batch := range w.records {
		for _, rec := range batch {
			// Проверка внутри батча, а не только на приёме: жёсткая отмена
			// должна обрывать разбор уже полученного буфера, иначе Stop по
			// истечении бюджета ждал бы обработки всей очереди.
			if ctx.Err() != nil {
				return
			}

			// Отравленная партиция: записи вычитываются из очереди, но не
			// обрабатываются. Выбрасывать их безопасно ровно потому, что
			// оффсет не отмечен — после ребаланса или перезапуска они приедут
			// снова. А продолжать читать канал обязательно: перестань воркер
			// это делать, dispatch упёрся бы в полную очередь и заблокировал
			// общий цикл опроса, остановив заодно и здоровые партиции.
			if w.poisoned {
				continue
			}

			c.processRecord(ctx, client, rec, key, w, logger)
		}
	}
}

// processRecord проводит одну запись через трейсинг, обработчик и отметку
// к коммиту.
func (c *KafkaConsumer) processRecord(
	ctx context.Context, client *kgo.Client, rec *kgo.Record,
	key workerKey, w *partitionWorker, logger *slog.Logger,
) {
	defer func() {
		if r := recover(); r != nil {
			// Отдельный перехват вокруг обвязки: паника в трейсинге или в
			// метриках не должна уносить воркера вместе с очередью.
			c.panics.report(ctx, panicSiteProcessMessage, r, debug.Stack(), recordAttrs(rec)...)
		}
	}()

	// Trace context из заголовков записи kotel уже извлёк на хуке фетча,
	// поэтому ручного propagator-carrier здесь нет.
	_, span := c.telemetry.tracer.WithProcessSpan(rec)
	defer span.End()

	// Контекст спана построен от rec.Context, у которого нет отмены. Обработчику
	// нужен отменяемый контекст воркера, поэтому спан переносится в него, а не
	// наоборот.
	msgCtx := trace.ContextWithSpan(ctx, span)

	log := logger.With(slog.Int64("offset", rec.Offset))
	if sc := span.SpanContext(); sc.IsValid() {
		log = log.With(slog.String("trace_id", sc.TraceID().String()))
	}

	handler, ok := c.handler(rec.Topic)
	if !ok {
		// Возможно только при рассинхроне подписки и карты обработчиков.
		// Оффсет не отмечается: сообщение вернётся, а не исчезнет.
		log.Error("No handler registered for topic")
		c.countMessage(msgCtx, rec.Topic, consumerStatusError)
		c.poison(client, key, w, log, errors.New("no handler registered"))

		return
	}

	msg := newIncomingMessage(rec)

	start := time.Now()

	decided, err := c.runHandler(msgCtx, handler, msg, span, log)
	if !decided {
		// Отмена застала паузу между попытками: вердикта нет, длительность
		// мерить нечего.
		c.countMessage(msgCtx, rec.Topic, consumerStatusSkipped)

		return
	}

	status := consumerStatusSuccess
	if err != nil {
		status = c.resolveFailure(msgCtx, client, msg, key, w, err, log)
	}

	// Длительность включает все попытки и все паузы между ними: измеряется
	// задержка сообщения, а не одного вызова обработчика.
	c.metrics.duration.Record(msgCtx, time.Since(start).Seconds(),
		metric.WithAttributes(attribute.String("topic", rec.Topic), attribute.String("status", status)))
	c.countMessage(msgCtx, rec.Topic, status)

	if status == consumerStatusError {
		// Неотмеченная запись — и есть гарантия at-least-once: коммит не
		// сдвинется за неё, и после перезапуска или ребаланса она приедет
		// снова. Партиция при этом уже отравлена в resolveFailure, иначе
		// отметка следующей записи сдвинула бы коммит за эту.
		return
	}

	// MarkCommitRecords без AutoCommitMarks() был бы no-op, а сдвинуть оффсет
	// назад он не умеет — порядок отметок внутри партиции гарантирован тем,
	// что воркер один.
	client.MarkCommitRecords(rec)
}

// resolveFailure решает судьбу сообщения, которое обработчик не осилил за все
// попытки: отдать его OnMessageSkipped или остановить партицию.
//
// Возвращает статус для метрик. consumerStatusError означает, что оффсет
// отмечать нельзя и партиция отравлена; consumerStatusSkipped — что хук забрал
// сообщение и коммит может двигаться дальше.
func (c *KafkaConsumer) resolveFailure(
	ctx context.Context,
	client *kgo.Client,
	msg IncomingMessage,
	key workerKey,
	w *partitionWorker,
	cause error,
	log *slog.Logger,
) string {
	if c.config.OnMessageSkipped == nil {
		log.Error("Message processing failed and no OnMessageSkipped hook is configured",
			slog.Any("error", cause))
		c.poison(client, key, w, log, cause)

		return consumerStatusError
	}

	if hookErr := c.callSkipHook(ctx, msg, cause); hookErr != nil {
		log.Error("OnMessageSkipped refused the message",
			slog.Any("error", cause),
			slog.Any("hook_error", hookErr))
		c.poison(client, key, w, log, cause)

		return consumerStatusError
	}

	log.Warn("Message skipped after exhausting retries", slog.Any("error", cause))

	return consumerStatusSkipped
}

// callSkipHook вызывает OnMessageSkipped под собственным recover.
//
// Хук — чужой код, исполняемый в горутине воркера уже после того, как recover
// вокруг обработчика отработал: его собственная паника прошла бы мимо и уронила
// процесс. Паника трактуется как отказ забрать сообщение — иначе упавший хук
// молча разрешил бы сдвинуть коммит.
func (c *KafkaConsumer) callSkipHook(ctx context.Context, msg IncomingMessage, cause error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			c.panics.report(ctx, panicSiteMessageSkipped, r, debug.Stack(),
				slog.String("topic", msg.Topic),
				slog.Int("partition", int(msg.Partition)),
				slog.Int64("offset", msg.Offset))

			err = fmt.Errorf("on message skipped: %w", ErrHandlerPanic)
		}
	}()

	return c.config.OnMessageSkipped(ctx, msg, cause)
}

// poison останавливает партицию на неотмеченном оффсете.
//
// Флага poisoned мало: без паузы клиент продолжил бы тянуть записи, которые
// воркер обязан выбрасывать, и партиция крутила бы трафик, который всё равно
// приедет заново. PauseFetchPartitions прекращает выборку, не отдавая
// партицию: назначение остаётся за нами, лаг растёт и виден в мониторинге.
//
// Пауза снимается следующим назначением этой партиции — своим (ребаланс,
// перезапуск) или чужим — ровно тогда, когда она начнёт читаться с
// проваленного оффсета заново. Снимает её onPartitionsAssigned: сам по себе
// ребаланс приостановленную партицию не отпускает, набор пауз в franz-go
// принадлежит клиенту, а не назначению.
func (c *KafkaConsumer) poison(
	client *kgo.Client, key workerKey, w *partitionWorker, log *slog.Logger, cause error,
) {
	w.poisoned = true

	log.Error("Partition paused at uncommitted offset; the message will be redelivered "+
		"after rebalance or restart",
		slog.Any("error", cause))

	client.PauseFetchPartitions(map[string][]int32{key.topic: {key.partition}})
}

// countMessage инкрементирует счётчик исходов обработки.
func (c *KafkaConsumer) countMessage(ctx context.Context, topic, status string) {
	c.metrics.processed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("status", status)))
}

// runHandler вызывает обработчик с повторами.
//
// Первый результат — «вердикт получен»: false означает, что отмена контекста
// прервала паузу между попытками и исход сообщения неизвестен.
func (c *KafkaConsumer) runHandler(
	ctx context.Context, handler ConsumerHandler, msg IncomingMessage, span trace.Span, log *slog.Logger,
) (bool, error) {
	maxRetries := c.config.Consumer.HandlerMaxRetries

	for attempt := 0; ; attempt++ {
		err := c.callHandler(ctx, handler, msg, span)
		if err == nil {
			return true, nil
		}

		// Отрицательное значение (-1) означает «повторять бесконечно», ноль —
		// «без повторов»: attempt считает уже сделанные повторы, а не вызовы.
		if maxRetries >= 0 && attempt >= maxRetries {
			log.Error("Handler failed, giving up",
				slog.Int("attempts", attempt+1),
				slog.Any("error", err))
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			return true, err
		}

		log.Warn("Handler failed, retrying",
			slog.Int("attempt", attempt+1),
			slog.Int("max_retries", maxRetries),
			slog.Any("error", err))
		c.metrics.retries.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", msg.Topic)))

		if !waitRetryDelay(ctx, c.config.Consumer.HandlerRetryDelay) {
			return false, err
		}
	}
}

// callHandler вызывает обработчик под recover.
//
// Паника превращается в обычную ошибку, чтобы сообщение прошло штатный путь
// повторов, а воркер остался жив: до этого паника обработчика убивала воркера
// и осиротевшая очередь целиком перепрыгивалась коммитом следующего воркера.
// Плата — детерминированная паника повторяется HandlerMaxRetries раз.
func (c *KafkaConsumer) callHandler(
	ctx context.Context, handler ConsumerHandler, msg IncomingMessage, span trace.Span,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrHandlerPanic, r)
			span.RecordError(err)
			c.panics.report(ctx, panicSiteHandler, r, debug.Stack(),
				slog.String("topic", msg.Topic),
				slog.Int("partition", int(msg.Partition)),
				slog.Int64("offset", msg.Offset))
		}
	}()

	return handler.ProcessMessage(ctx, msg)
}

// waitRetryDelay выдерживает паузу; false означает отмену контекста.
func waitRetryDelay(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// onPartitionsAssigned заводит воркеров назначенных партиций.
//
// Снятие паузы — обязательная часть назначения, а не подстраховка. Набор
// приостановленных партиций в franz-go живёт на уровне клиента и переживает
// ребаланс: методы Pause*/Resume* — единственное, что его меняет. Без этого
// вызова партиция, отравленная и приостановленная в poison, при возврате к
// тому же экземпляру получала бы свежего воркера с poisoned=false, но её
// фетч оставался бы выключенным навсегда — и обещание «сообщение приедет
// заново после ребаланса» держалось бы только при переезде на другой процесс.
func (c *KafkaConsumer) onPartitionsAssigned(_ context.Context, client *kgo.Client, assigned map[string][]int32) {
	client.ResumeFetchPartitions(assigned)

	for topic, partitions := range assigned {
		for _, partition := range partitions {
			c.worker(client, workerKey{topic: topic, partition: partition})
		}
	}

	c.logger.Info("Partitions assigned", slog.Any("partitions", assigned))
}

// onPartitionsRevoked останавливает воркеров отзываемых партиций и фиксирует
// их оффсеты.
//
// Колбэк блокирует ребаланс, и это ровно то, что нужно: пока он не вернулся,
// партиция не уедет к другому участнику группы.
//
// Коммит здесь обязателен, а не «на всякий случай»: собственный
// OnPartitionsRevoked отключает встроенный defaultRevoke franz-go вместе с его
// финальным синхронным коммитом, и без явного вызова отмеченные оффсеты
// потерялись бы вместе с сессией.
func (c *KafkaConsumer) onPartitionsRevoked(ctx context.Context, client *kgo.Client, revoked map[string][]int32) {
	drainCtx, cancelDrain := c.rebalanceBudget(ctx)
	c.stopWorkers(drainCtx, revoked)
	cancelDrain()

	// Отдельный бюджет, а не остаток от drainCtx: если воркеров пришлось
	// добивать по таймауту, тот контекст уже отменён, и коммит провалился бы
	// мгновенно — потеряв ровно те оффсеты, ради которых колбэк и написан.
	commitCtx, cancelCommit := c.rebalanceBudget(ctx)
	defer cancelCommit()

	if err := client.CommitMarkedOffsets(commitCtx); err != nil {
		c.logger.Error("Failed to commit marked offsets on revoke",
			slog.Any("partitions", revoked),
			slog.Any("error", err))

		return
	}

	c.logger.Info("Partitions revoked", slog.Any("partitions", revoked))
}

// onPartitionsLost останавливает воркеров потерянных партиций.
//
// Коммита здесь нет намеренно: партиции потеряны вместе с сессией группы, и
// коммит либо будет отвергнут координатором, либо перезапишет оффсет,
// принадлежащий уже другому участнику.
func (c *KafkaConsumer) onPartitionsLost(ctx context.Context, _ *kgo.Client, lost map[string][]int32) {
	ctx, cancel := c.rebalanceBudget(ctx)
	defer cancel()

	c.stopWorkers(ctx, lost)

	c.logger.Warn("Partitions lost", slog.Any("partitions", lost))
}

// rebalanceBudget ограничивает время, которое колбэк ребаланса проводит в
// ожидании воркеров.
//
// franz-go передаёт в колбэки контекст жизни клиента, а не контекст ребаланса:
// он отменяется только при закрытии клиента. Без собственного дедлайна
// зависший обработчик держал бы колбэк дольше RebalanceTimeout, координатор
// исключил бы участника из группы, и вместо управляемого отзыва партиций
// случился бы onLost — то есть худший из двух исходов наступал бы сам собой.
func (c *KafkaConsumer) rebalanceBudget(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.config.Consumer.RebalanceTimeout)
}

// stopWorkers мягко останавливает воркеров перечисленных партиций и дожидается
// их выхода.
func (c *KafkaConsumer) stopWorkers(ctx context.Context, partitions map[string][]int32) {
	stopped := make([]workerKey, 0, len(partitions))

	// Сначала закрываются все очереди, и лишь потом идёт ожидание: иначе
	// воркеры дренировались бы по очереди, а не параллельно.
	for topic, parts := range partitions {
		for _, partition := range parts {
			key := workerKey{topic: topic, partition: partition}
			if w, ok := c.workers[key]; ok {
				w.stop()

				stopped = append(stopped, key)
			}
		}
	}

	c.awaitWorkers(ctx, stopped)
}

// stopAllWorkers мягко останавливает всех воркеров.
func (c *KafkaConsumer) stopAllWorkers(ctx context.Context) {
	stopped := make([]workerKey, 0, len(c.workers))

	for key, w := range c.workers {
		w.stop()

		stopped = append(stopped, key)
	}

	c.awaitWorkers(ctx, stopped)
}

// awaitWorkers ждёт завершения воркеров и убирает их из карты.
//
// Воркер, не уложившийся в бюджет, отменяется жёстко: продолжать обработку
// партиции, которая уже отдана другому участнику группы, хуже, чем оборвать
// текущее сообщение — оно всё равно не отмечено и приедет снова.
func (c *KafkaConsumer) awaitWorkers(ctx context.Context, keys []workerKey) {
	for _, key := range keys {
		w := c.workers[key]
		delete(c.workers, key)

		select {
		case <-w.done:
		case <-ctx.Done():
			c.logger.Warn("Partition worker did not stop in time, cancelling",
				slog.String("topic", key.topic),
				slog.Int("partition", int(key.partition)))
			w.cancel()
		}
	}
}

// Stop останавливает консьюмера и закрывает клиента.
//
// Порядок фиксирован: остановить цикл опроса, дождаться воркеров, явно
// закоммитить отмеченные оффсеты (не полагаясь на тикер автокоммита) и лишь
// затем закрыть клиента. Весь путь ограничен Config.GracefulTimeout; при
// исчерпании бюджета в лог уходит предупреждение, и завершение продолжается.
//
// Закрытие — только CloseAllowingRebalance: обычный Close при
// BlockRebalanceOnPoll повисает, потому что уход из группы вызывает ребаланс,
// заблокированный незавершённым опросом.
//
// Клиент закрывается и при исчерпании бюджета, даже если воркер ещё жив:
// отметка оффсета после закрытия — безопасный no-op, а не обращение к
// освобождённым ресурсам, поэтому удерживать хендл ради опоздавших воркеров
// не нужно.
//
// Идемпотентен: повторный вызов пишет предупреждение и возвращает nil.
func (c *KafkaConsumer) Stop() error {
	if !c.stopping.CompareAndSwap(false, true) {
		c.logger.Warn("Consumer is already stopping")

		return nil
	}

	c.mu.Lock()
	client, pollCancel := c.client, c.pollCancel
	c.mu.Unlock()

	if client == nil {
		// Start не вызывался или не дошёл до создания клиента.
		c.lifeCancel()

		return nil
	}

	c.logger.Info("Starting kafka consumer shutdown")

	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.lifeCtx), c.config.GracefulTimeout)
	defer cancel()

	pollCancel()

	if waitClosed(ctx, c.loopDone) {
		c.stopAllWorkers(ctx)
	} else {
		// Карта воркеров не защищена мьютексом и принадлежит циклу опроса:
		// пока цикл жив, трогать её нельзя, поэтому остаётся жёсткая отмена.
		c.logger.Warn("Poll loop did not stop within graceful timeout",
			slog.Duration("timeout", c.config.GracefulTimeout))
		c.lifeCancel()
	}

	// Отдельный бюджет вместо остатка от ctx: ровно в том случае, когда
	// финальный коммит важнее всего — цикл опроса или воркеры не уложились в
	// GracefulTimeout, — остаток равен нулю, и коммит провалился бы, не сходив
	// к брокеру. Одна операция к координатору стоит не дороже ребаланса,
	// поэтому и бюджет тот же.
	commitCtx, cancelCommit := context.WithTimeout(
		context.WithoutCancel(c.lifeCtx), c.config.Consumer.RebalanceTimeout)
	defer cancelCommit()

	var err error

	if commitErr := client.CommitMarkedOffsets(commitCtx); commitErr != nil {
		c.logger.Error("Failed to commit marked offsets on shutdown", slog.Any("error", commitErr))
		err = fmt.Errorf("committing marked offsets: %w", commitErr)
	}

	client.CloseAllowingRebalance()
	c.lifeCancel()

	c.logger.Info("Kafka consumer shutdown completed")

	return err
}

// waitClosed ждёт закрытия канала; false означает исчерпание бюджета.
func waitClosed(ctx context.Context, done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

// newIncomingMessage переводит запись franz-go в сообщение публичного API.
func newIncomingMessage(rec *kgo.Record) IncomingMessage {
	return IncomingMessage{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Key:       rec.Key,
		Value:     rec.Value,
		Headers:   fromRecordHeaders(rec.Headers),
		Timestamp: rec.Timestamp,
	}
}

// recordAttrs — координаты записи для логов.
func recordAttrs(rec *kgo.Record) []slog.Attr {
	return []slog.Attr{
		slog.String("topic", rec.Topic),
		slog.Int("partition", int(rec.Partition)),
		slog.Int64("offset", rec.Offset),
	}
}
