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
	// consumerStatusSkipped — запись сознательно пропущена и отмечена к
	// коммиту: обработчик исчерпал повторы, а OnMessageSkipped вернул nil,
	// то есть принял её на себя.
	//
	// Это единственный статус, при котором коммит сдвигается за запись, так и
	// не обработанную успешно. Ненулевой рост — повод смотреть в OnMessageSkipped.
	consumerStatusSkipped = "skipped"
	// consumerStatusCancelled — отмена контекста застала паузу между повторами:
	// вердикта нет, оффсет НЕ отмечен, партицию останавливать не за что.
	// Сообщение приедет снова.
	//
	// Отдельно от skipped, хотя оба означают «успеха не было»: коммит здесь не
	// двигается, записи в DLQ под это событие нет, и происходит оно на каждом
	// штатном завершении процесса. Свалив их в одно значение, дашборд DLQ
	// завышал бы счёт на каждом деплое. Длительность под этим статусом не
	// пишется: мерить нечего, обработка не закончилась.
	consumerStatusCancelled = "cancelled"
	// consumerStatusDropped — запись прочитана из очереди отравленной партиции
	// и выброшена не глядя. Оффсет не отмечен, поэтому она приедет снова.
	//
	// Считается, чтобы масштаб отравления был виден: за одной остановившейся
	// партицией стоит не одно сообщение, а весь буфер, набранный до паузы.
	consumerStatusDropped = "dropped"
)

// consumerStatuses — то же множество списком, для прогрева кэша опций метрик.
//
// Держится рядом с константами намеренно: забытый здесь статус не сломает
// метрику, но вернёт ей аллокации на горячем пути — молча, и заметно это будет
// только по бенчмарку.
var consumerStatuses = []string{
	consumerStatusSuccess,
	consumerStatusError,
	consumerStatusSkipped,
	consumerStatusCancelled,
	consumerStatusDropped,
}

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
	processed        metric.Int64Counter
	duration         metric.Float64Histogram
	retries          metric.Int64Counter
	fetchErrors      metric.Int64Counter
	groupErrors      metric.Int64Counter
	workersActive    metric.Int64UpDownCounter
	partitionsPaused metric.Int64UpDownCounter
	panics           metric.Int64Counter
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
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(consumerDurationBuckets...))
	m.duration = record(&reg, durationName, duration, err)

	retriesName := "kafkax.consumer.handler.retries"
	retries, err := meter.Int64Counter(retriesName,
		metric.WithDescription("Handler invocations that failed and were retried"))
	m.retries = record(&reg, retriesName, retries, err)

	// Считаются эпизоды, а не опросы: неретраибельную ошибку партиции franz-go
	// возвращает на каждом опросе заново, и счётчик всех вхождений мерил бы
	// частоту опроса, а не масштаб проблемы. Инкремент делается на переходе
	// «партиция была здорова → сломалась» и на смену текста ошибки.
	fetchErrorsName := "kafkax.consumer.fetch.errors"
	fetchErrors, err := meter.Int64Counter(fetchErrorsName,
		metric.WithDescription("Partition-level fetch error episodes, counted on state change"))
	m.fetchErrors = record(&reg, fetchErrorsName, fetchErrors, err)

	// Отказ уровня группы — не то же самое, что отказ партиции: сообщений нет
	// вообще, и алерт на него нужен свой. franz-go подкидывает такую ошибку
	// синтетическим фетчем с пустым топиком и партицией 0, поэтому в
	// fetch.errors она выглядела как поломка несуществующей партиции.
	// Атрибутов нет намеренно: топика у этой ошибки не существует.
	groupErrorsName := "kafkax.consumer.group.errors"
	groupErrors, err := meter.Int64Counter(groupErrorsName,
		metric.WithDescription("Group session error episodes, counted on state change"))
	m.groupErrors = record(&reg, groupErrorsName, groupErrors, err)

	workersName := "kafkax.consumer.workers.active"
	workers, err := meter.Int64UpDownCounter(workersName,
		metric.WithDescription("Partition workers currently running"))
	m.workersActive = record(&reg, workersName, workers, err)

	// Продолжающийся сигнал для штатного исхода политики отравленного
	// сообщения. Разового лога мало: приостановленная партиция — это состояние,
	// и алерт «стоит хотя бы одна» строится только по гейджу. Разовое событие к
	// моменту дежурства уже уехало из окна.
	pausedName := "kafkax.consumer.partitions.paused"
	paused, err := meter.Int64UpDownCounter(pausedName,
		metric.WithDescription("Partitions currently paused at an uncommitted offset"))
	m.partitionsPaused = record(&reg, pausedName, paused, err)

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

	// opts — готовые опции атрибутов доменных метрик. Прогревается в
	// AddHandler и после Start не растёт: множество топиков замкнуто
	// обработчиками, множество статусов — константами consumerStatus*.
	opts *optsCache

	// workersMu защищает саму карту. kgo.BlockRebalanceOnPoll делает опрос и
	// колбэки ребаланса взаимно исключающими, но полагаться на это как на
	// единственную синхронизацию нельзя: колбэки исполняются в горутинах
	// franz-go, и на любой ошибке в порядке остановки инвариант «карта
	// принадлежит циклу опроса» превращается в гонку. Мьютекс стоит одного
	// незанятого захвата на батч.
	//
	// Держать его на время ожидания воркеров запрещено: остановка снимает
	// нужных воркеров с карты под мьютексом, а ждёт их уже без него.
	workersMu sync.Mutex
	workers   map[workerKey]*partitionWorker

	// pausedMu защищает набор приостановленных партиций и журнал последних
	// сообщённых ошибок фетча. Отдельный мьютекс, а не workersMu: оба поля
	// трогаются из цикла опроса и из горутин воркеров, но к карте воркеров
	// отношения не имеют, а брать её замок на пути ошибки фетча значило бы
	// сцепить разбор ошибок с созданием воркеров.
	pausedMu sync.Mutex

	// paused — партиции, снятые с выборки в poison. Дублирует набор пауз внутри
	// franz-go намеренно: тот набор непрозрачен, а без собственного счёта
	// нельзя ни отличить повторный poison от первого (иначе гейдж
	// partitions.paused считал бы сообщения, а не партиции), ни узнать, что
	// снятие паузы действительно что-то сняло.
	paused map[workerKey]struct{}

	// lastFetchErr — последняя сообщённая ошибка на партицию, по тексту.
	// Неретраибельную ошибку franz-go возвращает на каждом опросе заново;
	// без этого журнала лог и счётчик мерили бы частоту опроса. Ключ
	// workerKey{} с пустым топиком отведён под ошибки уровня группы: топика у
	// них нет, а состояние «группа сломана» точно так же требует дедупа.
	lastFetchErr map[workerKey]string

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

	started atomic.Bool

	// stopping взводится в начале остановки и читается Start: он обязан узнать
	// о ней раньше, чем опубликует созданного клиента.
	stopping atomic.Bool

	// stopOnce делает Stop одновременно однократным и блокирующим: второй
	// вызывающий ждёт первого и получает тот же результат, а не nil «уже
	// останавливаемся». Без этого отмена контекста Start, которая теперь и
	// сама зовёт Stop, отнимала бы у явного Stop любую возможность узнать,
	// закончилось завершение или ещё идёт.
	stopOnce sync.Once
	stopErr  error
}

// NewKafkaConsumer создаёт консьюмера.
//
// Соединения здесь не устанавливаются и горутины не запускаются: набор топиков
// известен только после AddHandler, а franz-go требует его при создании
// клиента, поэтому сам клиент создаётся в Start. Конструктор проверяет
// конфигурацию, готовит логгер, метрики и репортер паник.
func NewKafkaConsumer(config Config) (*KafkaConsumer, error) {
	const op = "new_kafka_consumer"

	// Не оборачивается: у агрегата валидации Unwrap() []error, и fmt.Errorf
	// подменил бы его на Unwrap() error — документированный разбор списка
	// перестал бы работать ровно там, где он нужен.
	if err := config.validateConsumer(); err != nil {
		return nil, err
	}

	logger := config.logger("kafka_consumer").With(slog.String("group", config.Consumer.Group))

	metrics, err := newConsumerMetrics(otel.Meter(instrumentationName))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	lifeCtx, lifeCancel := context.WithCancel(context.Background())

	return &KafkaConsumer{
		config:       config,
		logger:       logger,
		telemetry:    newTelemetry(config.ClientID, config.Consumer.Group),
		metrics:      metrics,
		panics:       panicReporter{logger: logger, panics: metrics.panics, onPanic: config.OnPanic},
		handlers:     make(map[string]ConsumerHandler),
		opts:         newOptsCache(0),
		workers:      make(map[workerKey]*partitionWorker),
		paused:       make(map[workerKey]struct{}),
		lastFetchErr: make(map[workerKey]string),
		lifeCtx:      lifeCtx,
		lifeCancel:   lifeCancel,
		loopDone:     make(chan struct{}),
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
		return fmt.Errorf("add handler for topic %q: %w", topic, ErrDuplicateHandler)
	}

	// Цепочка middleware собирается один раз при регистрации, а не на каждое
	// сообщение: аллокации замыканий на горячем пути ничего не дают.
	c.handlers[topic] = Chain(handler, mws...)

	// По той же причине здесь прогреваются опции метрик: набор атрибутов
	// топика известен целиком уже сейчас, и строить его заново на каждое
	// сообщение незачем.
	c.opts.warm(topic, consumerStatuses...)

	return nil
}

// Start создаёт клиента Kafka и запускает цикл опроса. Не блокирует.
//
// Отмена ctx запускает ровно тот же путь, что и Stop, — с дренажем очередей и
// финальным коммитом. Разница только в том, что ошибку завершения при этом
// некому вернуть: она уходит в лог. Предпочтительнее явный Stop.
//
// Повторный вызов возвращает ErrConsumerStarted; консьюмер, прошедший Stop, не
// перезапускается.
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

	// Быстрый путь: Stop уже прошёл, создавать клиента незачем. Гарантию даёт
	// не эта проверка, а повторная — под c.mu, рядом с публикацией клиента.
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

	// Проверка stopping и публикация клиента — одна критическая секция, и это
	// обязательное условие, а не аккуратность. Stop взводит stopping до захвата
	// c.mu, поэтому разнести их значит открыть окно, в котором Stop видит
	// c.client == nil, уходит по ранней ветке и оставляет уже созданного
	// клиента — присоединившегося к группе, с живым heartbeat — без единого
	// владельца, способного его закрыть. Типовой триггер — SIGTERM во время
	// старта пода.
	c.mu.Lock()
	if c.stopping.Load() {
		c.mu.Unlock()
		pollCancel()
		client.CloseAllowingRebalance()
		c.started.Store(false)

		return ErrConsumerClosed
	}

	c.client = client
	c.pollCancel = pollCancel
	c.mu.Unlock()

	go c.watchContext(ctx)
	go c.runPollLoop(pollCtx, client)

	c.logger.Info("Kafka consumer started", slog.Any("topics", topics))

	return nil
}

// watchContext переводит отмену пользовательского ctx в штатную остановку.
// Сам завершается вместе с жизненным циклом, чтобы не пережить консьюмера.
//
// Именно Stop, а не голый lifeCancel: отмена контекста обязана оставлять после
// себя закрытого клиента и закоммиченные оффсеты. Без этого экземпляр
// превращался бы в зомби — heartbeat идёт, партиции закреплены, координатор
// ждёт его на каждом ребалансе, а закрыть клиента уже некому.
func (c *KafkaConsumer) watchContext(ctx context.Context) {
	select {
	case <-ctx.Done():
		// Возврат некуда отдать: контекст отменил не тот, кто ждёт ошибку.
		if err := c.Stop(); err != nil {
			c.logger.Error("Shutdown on context cancellation failed", slog.Any("error", err))
		}
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
//
// Перехват паники здесь — страховка, а не лечение: все чужие вызовы на этом
// пути уже под собственными recover. Но именно эта горутина владеет картой
// воркеров и гейтом BlockRebalanceOnPoll, и её падение уронило бы процесс
// целиком, поэтому непойманного пути отсюда быть не должно. Консьюмер после
// такого перестаёт потреблять: закрытие loopDone разблокирует Stop, но сам
// Stop обязан позвать вызывающий.
func (c *KafkaConsumer) runPollLoop(ctx context.Context, client *kgo.Client) {
	// Порядок регистрации обратен порядку исполнения: сначала перехват паники,
	// и только затем сигнал о завершении — иначе Stop пошёл бы закрывать
	// клиента, пока паника ещё разматывается.
	defer close(c.loopDone)
	defer func() {
		if r := recover(); r != nil {
			c.panics.report(context.WithoutCancel(ctx), panicSitePollLoop, r, debug.Stack())
		}
	}()

	for c.pollOnce(ctx, client) {
	}
}

// pollOnce обрабатывает результат одного опроса. false означает, что цикл
// закончился.
//
// Отдельная функция ради defer: AllowRebalance обязателен на каждой итерации, а
// в теле цикла есть два ранних выхода и возможная паника, которые его миновали
// бы. Без него следующий ребаланс — включая тот, что инициирует закрытие
// клиента, — повис бы навсегда.
func (c *KafkaConsumer) pollOnce(ctx context.Context, client *kgo.Client) bool {
	defer client.AllowRebalance()

	fetches := client.PollRecords(ctx, c.config.Consumer.MaxPollRecords)

	// Оба условия проверяются до разбора ошибок: закрытие клиента и отмена
	// контекста приезжают синтетическим фетчем с ошибкой в нулевой
	// партиции, и принимать их за отказ брокера не за чем.
	if fetches.IsClientClosed() {
		return false
	}

	if err := fetches.Err0(); errors.Is(err, context.Canceled) {
		return false
	}

	// Обход ошибок вынесен наружу цикла по записям намеренно: партиция с
	// фатальной ошибкой и без записей приезжает отдельным пустым фетчем и
	// изнутри обхода Records не видна вовсе.
	fetches.EachError(c.reportFetchError)

	fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
		c.dispatch(ctx, client, ftp)
	})

	return true
}

// reportFetchError фиксирует ошибку фетча — партиционную или групповую.
//
// Сообщается только смена состояния. Ретраибельные ошибки franz-go гасит сам,
// а неретраибельные оставляет в фетче: курсор снова становится годным, и
// следующий опрос приносит ту же ошибку. Бэкоффа в этой ветке нет, поэтому
// безусловный лог давал бы поток записей Error с частотой опроса — при
// MaxWait=500ms это минимум две в секунду на партицию, а при мгновенном ответе
// брокера на порядки больше. Счётчик при этом мерил бы частоту опроса вместо
// масштаба проблемы.
func (c *KafkaConsumer) reportFetchError(topic string, partition int32, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed) {
		return
	}

	// Отказ уровня группы franz-go подкидывает синтетическим фетчем с пустым
	// топиком и партицией 0. Без разбора он выглядел бы как поломка
	// несуществующей партиции 0 топика "" — притом что это худший из отказов
	// консьюмера: сообщений нет вообще, ни по одной партиции.
	if groupErr, isGroupErr := errors.AsType[*kgo.ErrGroupSession](err); isGroupErr {
		// Дедуп и лог смотрят на одну и ту же ошибку — распакованную. Иначе
		// смена обёртки вокруг неизменившейся причины считалась бы новым
		// эпизодом.
		if c.firstReport(workerKey{}, groupErr) {
			c.metrics.groupErrors.Add(context.WithoutCancel(c.lifeCtx), 1)
			c.logger.Error("Consumer group session error", slog.Any("error", groupErr))
		}

		return
	}

	key := workerKey{topic: topic, partition: partition}
	if !c.firstReport(key, err) {
		return
	}

	c.metrics.fetchErrors.Add(context.WithoutCancel(c.lifeCtx), 1,
		c.opts.get(topic, noStatus).add...)
	c.logger.Error("Partition fetch error",
		slog.String("topic", topic),
		slog.Int("partition", int(partition)),
		slog.Any("error", err))
}

// firstReport сообщает, изменилось ли состояние ошибки на key.
//
// Сравнение по тексту, а не по значению: ошибки franz-go — указатели на
// структуры, свежие на каждом фетче, и == сравнивал бы адреса. Текст же
// содержит и код ошибки, и её параметры, поэтому смена причины отказа
// распознаётся как новый эпизод.
func (c *KafkaConsumer) firstReport(key workerKey, err error) bool {
	text := err.Error()

	c.pausedMu.Lock()
	defer c.pausedMu.Unlock()

	if c.lastFetchErr[key] == text {
		return false
	}

	c.lastFetchErr[key] = text

	return true
}

// clearFetchError снимает отметку об ошибке: партиция снова отдаёт записи.
//
// Без сброса повторение той же ошибки после выздоровления не было бы сообщено
// вовсе — дедуп превратился бы в «сообщаем один раз за жизнь процесса».
func (c *KafkaConsumer) clearFetchError(key workerKey) {
	c.pausedMu.Lock()
	defer c.pausedMu.Unlock()

	delete(c.lastFetchErr, key)
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

	key := workerKey{topic: ftp.Topic, partition: ftp.Partition}

	// Партиция отдала записи — значит, выздоровела. Отметку о последней
	// сообщённой ошибке снимаем здесь, а не в обходе ошибок: тот про успех
	// ничего не знает.
	c.clearFetchError(key)

	worker := c.worker(client, key)

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
// окно, в котором фетч приезжает раньше колбэка, и стоит один незанятый захват
// мьютекса.
func (c *KafkaConsumer) worker(client *kgo.Client, key workerKey) *partitionWorker {
	c.workersMu.Lock()
	defer c.workersMu.Unlock()

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

	// Свежий воркер читает партицию с непрокоммиченного оффсета, поэтому пауза,
	// если она была, снимается именно здесь — и только здесь. Вызов под
	// workersMu: он берёт другой мьютекс и карту воркеров не трогает, а
	// вынесенный за замок открыл бы окно, в котором воркер уже в карте, но
	// партиция ещё не в выборке.
	c.resumePartition(client, key)

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

	for {
		// Отмена читается наравне с очередью, а не только внутри батча:
		// простаивающий воркер иначе не увидел бы её вовсе и висел бы на
		// приёме до тех пор, пока кто-нибудь не закроет канал.
		var batch []*kgo.Record

		select {
		case b, ok := <-w.records:
			if !ok {
				return
			}

			batch = b
		case <-ctx.Done():
			return
		}

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
				c.countMessage(context.WithoutCancel(ctx), rec.Topic, consumerStatusDropped)

				continue
			}

			c.processRecord(ctx, client, rec, key, w, logger)
		}
	}
}

// recordLogger — логгер одной записи, обогащаемый лениво.
//
// offset и trace_id нужны только сообщениям об отказе, а на happy path не
// пишется ни строки: два Logger.With клонируют хэндлер вместе с его
// предформатированными атрибутами, а TraceID().String() кодирует шестнадцать
// байт в hex — и всё это на каждое сообщение впустую. Обогащение поэтому
// откладывается до первого обращения и запоминается: путь отказа логирует по
// несколько раз, и платить за клонирование каждый раз незачем.
//
// Экземпляр живёт в пределах одного processRecord и не покидает горутину
// воркера, поэтому кэш без синхронизации.
type recordLogger struct {
	base   *slog.Logger
	offset int64

	// span проставляется уже после создания: обогащённый логгер нужен и
	// перехватчику паник, зарегистрированному раньше, чем спан появился.
	span trace.Span

	cached *slog.Logger
}

// get возвращает обогащённый логгер, строя его при первом обращении.
func (l *recordLogger) get() *slog.Logger {
	if l.cached != nil {
		return l.cached
	}

	log := l.base.With(slog.Int64("offset", l.offset))

	// Проверка на nil не формальность: сюда попадает и паника обвязки,
	// случившаяся до старта спана.
	if l.span != nil {
		if sc := l.span.SpanContext(); sc.IsValid() {
			log = log.With(slog.String("trace_id", sc.TraceID().String()))
		}
	}

	l.cached = log

	return log
}

// processRecord проводит одну запись через трейсинг, обработчик и отметку
// к коммиту.
func (c *KafkaConsumer) processRecord(
	ctx context.Context, client *kgo.Client, rec *kgo.Record,
	key workerKey, w *partitionWorker, logger *slog.Logger,
) {
	log := &recordLogger{base: logger, offset: rec.Offset}

	defer func() {
		r := recover()
		if r == nil {
			return
		}

		// Отдельный перехват вокруг обвязки: паника в трейсинге или в
		// метриках не должна уносить воркера вместе с очередью.
		c.panics.report(ctx, panicSiteProcessMessage, r, debug.Stack(), recordAttrs(rec)...)

		// Отравление здесь обязательно. Штатный возврат из processRecord
		// оставил бы запись без отметки, но не остановил бы партицию — и
		// первая же успешная запись за ней сдвинула бы коммит через
		// необработанную. Паника обвязки поэтому трактуется ровно как
		// исчерпание повторов.
		//
		// Метрика исхода намеренно не пишется: упасть могла как раз она, и
		// повторный вызов того же инструмента увёл бы панику из-под этого
		// recover. Машиночитаемый след даёт kafkax.consumer.panics с
		// site=process_message.
		c.poison(client, key, w, log, fmt.Errorf("panic in message processing: %v", r))
	}()

	// Trace context из заголовков записи kotel уже извлёк на хуке фетча,
	// поэтому ручного propagator-carrier здесь нет.
	_, span := c.telemetry.tracer.WithProcessSpan(rec)
	defer span.End()

	log.span = span

	// Контекст спана построен от rec.Context, у которого нет отмены. Обработчику
	// нужен отменяемый контекст воркера, поэтому спан переносится в него, а не
	// наоборот.
	msgCtx := trace.ContextWithSpan(ctx, span)

	handler, ok := c.handler(rec.Topic)
	if !ok {
		// Возможно только при рассинхроне подписки и карты обработчиков.
		// Оффсет не отмечается: сообщение вернётся, а не исчезнет.
		log.get().Error("No handler registered for topic")
		c.countMessage(msgCtx, rec.Topic, consumerStatusError)
		c.poison(client, key, w, log, errors.New("no handler registered"))

		return
	}

	msg := newIncomingMessage(rec)

	start := time.Now()

	decided, err := c.runHandler(msgCtx, handler, msg, span, log)
	if !decided {
		// Отмена застала паузу между попытками: вердикта нет, длительность
		// мерить нечего. Оффсет не отмечается — сообщение приедет снова, —
		// поэтому и статус не skipped: там коммит двигается, здесь нет.
		c.countMessage(context.WithoutCancel(msgCtx), rec.Topic, consumerStatusCancelled)

		return
	}

	status := consumerStatusSuccess
	if err != nil {
		status = c.resolveFailure(msgCtx, client, msg, key, w, err, log)
	}

	// Длительность включает все попытки и все паузы между ними: измеряется
	// задержка сообщения, а не одного вызова обработчика.
	c.recordOutcome(msgCtx, rec.Topic, status, time.Since(start))

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
	log *recordLogger,
) string {
	if c.config.OnMessageSkipped == nil {
		log.get().Error("Message processing failed and no OnMessageSkipped hook is configured",
			slog.Any("error", cause))
		c.poison(client, key, w, log, cause)

		return consumerStatusError
	}

	if hookErr := c.callSkipHook(ctx, msg, cause); hookErr != nil {
		log.get().Error("OnMessageSkipped refused the message",
			slog.Any("error", cause),
			slog.Any("hook_error", hookErr))
		c.poison(client, key, w, log, cause)

		return consumerStatusError
	}

	log.get().Warn("Message skipped after exhausting retries", slog.Any("error", cause))

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
// Пауза снимается вместе со сменой воркера — см. resumePartition. Набор пауз в
// franz-go принадлежит клиенту, а не назначению, и сам по себе ребаланс его не
// трогает.
func (c *KafkaConsumer) poison(
	client *kgo.Client, key workerKey, w *partitionWorker, log *recordLogger, cause error,
) {
	w.poisoned = true

	log.get().Error("Partition paused at uncommitted offset; the message will be redelivered "+
		"after rebalance or restart",
		slog.Any("error", cause))

	client.PauseFetchPartitions(map[string][]int32{key.topic: {key.partition}})

	c.pausedMu.Lock()
	defer c.pausedMu.Unlock()

	// Гейдж считает партиции, а не отравленные сообщения. Повторный poison той
	// же партиции возможен: воркер выбрасывает записи, но обвязка вокруг
	// processRecord может упасть и на выброшенной, — и без этой проверки
	// счётчик уехал бы вверх на каждой такой записи и никогда не вернулся.
	if _, already := c.paused[key]; already {
		return
	}

	c.paused[key] = struct{}{}
	c.metrics.partitionsPaused.Add(context.WithoutCancel(c.lifeCtx), 1)
}

// resumePartition возвращает партицию в выборку, если та была приостановлена.
//
// Вызывается ровно при появлении нового воркера — и это единственный момент,
// когда снимать паузу правильно. Новый воркер означает, что партиция будет
// прочитана заново с непрокоммиченного оффсета: отравившее её сообщение
// приедет снова, ради чего пауза и ставилась.
//
// Привязка к воркеру, а не к списку assigned, — не стилистический выбор.
// Балансировщик franz-go по умолчанию кооперативный, и onPartitionsAssigned
// получает только вновь добавленные партиции: партиция, пережившая ребаланс за
// тем же экземпляром, в этот список не попадает. Резюмировать по нему значило
// бы оставить её на паузе навсегда, если воркера всё-таки пересоздали, и
// наоборот — снять паузу с партиции, чей отравленный воркер жив и продолжает
// выбрасывать записи.
func (c *KafkaConsumer) resumePartition(client *kgo.Client, key workerKey) {
	c.pausedMu.Lock()

	_, wasPaused := c.paused[key]
	if wasPaused {
		delete(c.paused, key)
		c.metrics.partitionsPaused.Add(context.WithoutCancel(c.lifeCtx), -1)
	}

	// Журнал ошибок фетча чистится вместе с паузой: партиция начинает жизнь
	// заново, и следующая её поломка обязана быть сообщена как новая.
	delete(c.lastFetchErr, key)

	c.pausedMu.Unlock()

	if !wasPaused {
		return
	}

	client.ResumeFetchPartitions(map[string][]int32{key.topic: {key.partition}})
	c.logger.Info("Partition resumed after reassignment",
		slog.String("topic", key.topic),
		slog.Int("partition", int(key.partition)))
}

// recordOutcome записывает исход обработки целиком: длительность и счётчик.
//
// Отдельный метод, а не две строки в processRecord: это единственная точка,
// где на каждое сообщение трогаются оба инструмента разом, и её цена
// измеряется бенчмарком.
func (c *KafkaConsumer) recordOutcome(ctx context.Context, topic, status string, elapsed time.Duration) {
	// Один поиск в кэше на оба инструмента: набор атрибутов у них общий.
	opts := c.opts.get(topic, status)

	c.metrics.duration.Record(ctx, elapsed.Seconds(), opts.record...)
	c.metrics.processed.Add(ctx, 1, opts.add...)
}

// countMessage инкрементирует счётчик исходов обработки.
func (c *KafkaConsumer) countMessage(ctx context.Context, topic, status string) {
	c.metrics.processed.Add(ctx, 1, c.opts.get(topic, status).add...)
}

// runHandler вызывает обработчик с повторами.
//
// Первый результат — «вердикт получен»: false означает, что отмена контекста
// прервала паузу между попытками и исход сообщения неизвестен.
func (c *KafkaConsumer) runHandler(
	ctx context.Context, handler ConsumerHandler, msg IncomingMessage, span trace.Span, log *recordLogger,
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
			log.get().Error("Handler failed, giving up",
				slog.Int("attempts", attempt+1),
				slog.Any("error", err))
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			return true, err
		}

		log.get().Warn("Handler failed, retrying",
			slog.Int("attempt", attempt+1),
			slog.Int("max_retries", maxRetries),
			slog.Any("error", err))
		c.metrics.retries.Add(ctx, 1, c.opts.get(msg.Topic, noStatus).add...)

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
// Паузу снимает не этот колбэк, а создание воркера внутри c.worker — см.
// resumePartition. Балансировщик franz-go по умолчанию кооперативный, и
// assigned содержит только вновь добавленные партиции: снятие паузы по этому
// списку промахивалось бы мимо всех, кто остался за тем же экземпляром.
func (c *KafkaConsumer) onPartitionsAssigned(_ context.Context, client *kgo.Client, assigned map[string][]int32) {
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
	c.stopWorkers(drainCtx, client, revoked)
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
func (c *KafkaConsumer) onPartitionsLost(ctx context.Context, client *kgo.Client, lost map[string][]int32) {
	ctx, cancel := c.rebalanceBudget(ctx)
	defer cancel()

	c.stopWorkers(ctx, client, lost)

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

// keyedWorker — воркер вместе со своим ключом: снимок, снятый с карты под
// мьютексом, чтобы ждать воркеров, уже не удерживая его.
type keyedWorker struct {
	key    workerKey
	worker *partitionWorker
}

// stopWorkers мягко останавливает воркеров перечисленных партиций и дожидается
// их выхода.
func (c *KafkaConsumer) stopWorkers(ctx context.Context, client *kgo.Client, partitions map[string][]int32) {
	c.workersMu.Lock()

	stopped := make([]keyedWorker, 0, len(partitions))

	for topic, parts := range partitions {
		for _, partition := range parts {
			key := workerKey{topic: topic, partition: partition}
			if w, ok := c.workers[key]; ok {
				delete(c.workers, key)

				stopped = append(stopped, keyedWorker{key: key, worker: w})
			}
		}
	}

	c.workersMu.Unlock()

	c.closeAndAwait(ctx, client, stopped)
}

// stopAllWorkers мягко останавливает всех воркеров.
func (c *KafkaConsumer) stopAllWorkers(ctx context.Context, client *kgo.Client) {
	c.workersMu.Lock()

	stopped := make([]keyedWorker, 0, len(c.workers))

	for key, w := range c.workers {
		stopped = append(stopped, keyedWorker{key: key, worker: w})
	}

	clear(c.workers)
	c.workersMu.Unlock()

	c.closeAndAwait(ctx, client, stopped)
}

// closeAndAwait закрывает очереди снятых с карты воркеров и дожидается их
// выхода.
//
// Сначала закрываются все очереди, и лишь потом идёт ожидание: иначе воркеры
// дренировались бы по очереди, а не параллельно.
func (c *KafkaConsumer) closeAndAwait(ctx context.Context, client *kgo.Client, stopped []keyedWorker) {
	for _, kw := range stopped {
		kw.worker.stop()
	}

	c.awaitWorkers(ctx, stopped)

	// Пауза снимается вместе с воркером, который её поставил. Набор пауз в
	// franz-go живёт на уровне клиента и переживает и ребаланс, и отзыв
	// партиции: не сняв её здесь, мы оставили бы гейдж partitions.paused
	// поднятым за партицию, которой у нас уже нет, — то есть подняли бы алерт
	// на чужую проблему. Партиция, вернувшаяся к нам позже, получит свежего
	// воркера и будет прочитана заново в любом случае.
	for _, kw := range stopped {
		c.resumePartition(client, kw.key)
	}
}

// awaitWorkers ждёт завершения воркеров.
//
// Воркер, не уложившийся в бюджет, отменяется жёстко: продолжать обработку
// партиции, которая уже отдана другому участнику группы, хуже, чем оборвать
// текущее сообщение — оно всё равно не отмечено и приедет снова. Но и после
// отмены его выхода приходится дождаться: живой воркер продолжает отмечать
// оффсеты и трогать клиента параллельно с финальным коммитом и закрытием.
func (c *KafkaConsumer) awaitWorkers(ctx context.Context, stopped []keyedWorker) {
	i := 0
	for ; i < len(stopped); i++ {
		if !waitClosed(ctx, stopped[i].worker.done) {
			break
		}
	}

	if i == len(stopped) {
		return
	}

	// Бюджет исчерпан. Отмена идёт по всем оставшимся сразу, и лишь потом —
	// ожидание в одном общем жёстком бюджете: по отдельности худший случай
	// умножался бы на число воркеров.
	pending := stopped[i:]

	hardCtx, cancel := context.WithTimeout(
		context.WithoutCancel(c.lifeCtx), c.config.Consumer.RebalanceTimeout)
	defer cancel()

	for _, kw := range pending {
		c.logger.Warn("Partition worker did not stop in time, cancelling",
			slog.String("topic", kw.key.topic),
			slog.Int("partition", int(kw.key.partition)))
		kw.worker.cancel()
	}

	for _, kw := range pending {
		if !waitClosed(hardCtx, kw.worker.done) {
			c.logger.Error("Partition worker is still running after hard cancellation",
				slog.String("topic", kw.key.topic),
				slog.Int("partition", int(kw.key.partition)))
		}
	}
}

// Stop останавливает консьюмера и закрывает клиента.
//
// Порядок фиксирован и обязателен: остановить цикл опроса и дождаться его
// выхода, дождаться воркеров, явно закоммитить отмеченные оффсеты (не
// полагаясь на тикер автокоммита) и лишь затем закрыть клиента.
//
// Ждать цикл опроса приходится потому, что он владеет картой воркеров и гейтом
// BlockRebalanceOnPoll: CloseAllowingRebalance при живом цикле снял бы гейт,
// удерживаемый чужой горутиной, и запустил бы onPartitionsRevoked параллельно
// с dispatch — то есть гонку за картой и «send on closed channel» в горутине
// без вызывающего. Если цикл не вышел даже после жёсткой отмены, клиент
// остаётся открытым, а Stop возвращает ErrPollLoopStuck: утечка одного клиента
// дешевле падения процесса.
//
// Закрытие — только CloseAllowingRebalance: обычный Close при
// BlockRebalanceOnPoll повисает, потому что уход из группы вызывает ребаланс,
// заблокированный незавершённым опросом.
//
// Бюджет. Мягкая фаза (цикл опроса + дренаж воркеров) укладывается в
// Config.GracefulTimeout. Сверх него в худшем случае добавляются жёсткая
// добивка цикла, жёсткая добивка воркеров и финальный коммит — по
// Consumer.RebalanceTimeout каждая, — плюс время внутри
// CloseAllowingRebalance. Это стоит учитывать в terminationGracePeriodSeconds.
//
// Идемпотентен и блокирующий: завершение выполняется ровно один раз, а всякий
// вызывающий — включая пришедшего вторым и того, кто просто отменил контекст
// Start, — дожидается его конца и получает один и тот же результат.
func (c *KafkaConsumer) Stop() error {
	c.stopOnce.Do(func() { c.stopErr = c.shutdown() })

	return c.stopErr
}

// shutdown — тело остановки, выполняемое ровно один раз.
func (c *KafkaConsumer) shutdown() error {
	c.stopping.Store(true)

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

	if !c.awaitPollLoop(ctx) {
		c.logger.Error("Poll loop is still running after hard cancellation; " +
			"leaving the kafka client open to avoid racing it")

		return ErrPollLoopStuck
	}

	c.stopAllWorkers(ctx, client)

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
		err = fmt.Errorf("%w: %w", ErrCommitFailed, commitErr)
	}

	client.CloseAllowingRebalance()
	c.lifeCancel()

	c.logger.Info("Kafka consumer shutdown completed")

	return err
}

// awaitPollLoop дожидается выхода цикла опроса, при необходимости отменяя его
// жёстко. false означает, что цикл не вышел и трогать ни карту воркеров, ни
// клиента нельзя.
//
// Жёсткая отмена — это lifeCancel: она отменяет контексты воркеров, из-за чего
// разблокируются и dispatch, упёршийся в полную очередь, и сам воркер, если он
// стоял на приёме. Не помочь она может только тогда, когда цикл висит в чужом
// коде, отмену не проверяющем, — в slog.Handler или в экспортёре метрик.
func (c *KafkaConsumer) awaitPollLoop(ctx context.Context) bool {
	if waitClosed(ctx, c.loopDone) {
		return true
	}

	c.logger.Warn("Poll loop did not stop within graceful timeout, cancelling",
		slog.Duration("timeout", c.config.GracefulTimeout))
	c.lifeCancel()

	hardCtx, cancel := context.WithTimeout(
		context.WithoutCancel(c.lifeCtx), c.config.Consumer.RebalanceTimeout)
	defer cancel()

	return waitClosed(hardCtx, c.loopDone)
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
