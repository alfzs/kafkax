package kafkax

// consumer.go — публичный контракт консьюмера и его жизненный цикл:
// AddHandler, Start, Stop и состояния между ними.
//
// Граница с соседями. Внутренности вынесены: партиционная машинерия — в
// consumer_worker.go, колбэки ребаланса и остановка воркеров — в
// consumer_rebalance.go, инструменты OTel — в consumer_metrics.go.

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// IncomingMessage — сообщение Kafka, переданное в ConsumerHandler.
//
// # Владение памятью
//
// Key и Value — это срезы, разделяемые с записью franz-go, а не копии. Читать
// их после возврата из обработчика безопасно: пулинг буферов не включён
// (kgo.WithPools пакет не вызывает), поэтому память живёт ровно столько,
// сколько на неё ссылаются, как обычный объект под управлением GC.
//
// Мутировать их запрещено. Тот же самый срез пакет отдаёт повторно:
// в каждую попытку повтора (Consumer.HandlerMaxRetries) и затем в
// Config.OnMessageSkipped — уже после возврата из ProcessMessage. Обработчик,
// правящий Value на месте, увидит своё же изменение на следующей попытке и
// отдаст испорченное сообщение в DLQ.
//
// Копировать нужно не «чтобы пережить вызов», а чтобы изменить: append к Value
// с достаточной ёмкостью пишет в тот же массив. Для чтения, разбора и передачи
// в другой код копия не требуется.
//
// Отдельно про Config.ExtraOpts: kgo.WithPools, протащенный туда, включает в
// franz-go переиспользование буферов, а Record.Recycle пакет не вызывает.
// Такая конфигурация не поддерживается — пулинг в ней выродится в отсутствие
// возврата памяти, а не в порчу данных.
type IncomingMessage struct {
	Topic string
	// Partition — номер партиции, из которой прочитана запись. Порядок
	// сообщений гарантирован только внутри одной партиции: разные партиции
	// обрабатываются параллельными горутинами.
	Partition int32
	// Offset — позиция записи в партиции. Уникальна в паре с Partition и
	// монотонна внутри неё, поэтому годится ключом дедупликации на стороне
	// обработчика — того самого, без которого at-least-once превращается в
	// дубликаты.
	Offset int64
	// Key — ключ записи; nil, если сообщение отправлено без ключа. Именно он
	// определяет партицию, а значит и порядок, поэтому две записи с одним
	// ключом придут в одну партицию и будут обработаны последовательно.
	// Разбор составных ключей — в подпакете encoding. Про владение памятью
	// см. раздел выше.
	Key     []byte
	Value   []byte
	Headers Headers
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

var _ MessageConsumer = (*Consumer)(nil)

// consumerState — состояние жизненного цикла консьюмера.
//
// Три состояния, а не булев «запущен»: без отдельного терминального состояния
// Start после Stop неотличим от повторного Start и объясняет отказ неправдой —
// «уже запущен» вместо «остановлен». Разница существенна для вызывающего:
// первое лечится тем, что запуск не нужно повторять, второе — тем, что нужен
// новый консьюмер.
type consumerState int32

const (
	// consumerIdle — создан, но не запущен. Единственное состояние, из
	// которого разрешены AddHandler и Start.
	consumerIdle consumerState = iota
	// consumerRunning — цикл опроса работает.
	consumerRunning
	// consumerClosed — Stop начат или уже завершён. Терминальное состояние:
	// клиент franz-go закрыт, консьюмер не перезапускается.
	consumerClosed
)

// lifecycleErr описывает отказ операции, разрешённой только до старта.
// Для consumerIdle возвращает nil: отказывать не в чем.
func (s consumerState) lifecycleErr() error {
	if s == consumerClosed {
		return ErrConsumerClosed
	}

	if s == consumerRunning {
		return ErrConsumerStarted
	}

	return nil
}

// Consumer — консьюмер Kafka поверх franz-go.
//
// На каждую назначенную топик-партицию заводится горутина: обработка разных
// партиций идёт параллельно, внутри одной партиции — строго по порядку
// оффсетов. Методы безопасны для вызова из разных горутин.
type Consumer struct {
	config    Config
	logger    *slog.Logger
	telemetry telemetry
	metrics   consumerMetrics
	panics    panicReporter

	// handlers — неизменяемый снимок карты обработчиков, опубликованный
	// атомарно. Читается на каждое сообщение из всех воркеров партиций сразу,
	// пишется только AddHandler до Start.
	//
	// Замка на чтении нет намеренно. RWMutex.RLock — это атомарный инкремент
	// счётчика читателей, то есть запись в одну и ту же кэш-линию из всех
	// воркеров: измерено 38 ns/op под RunParallel против 10 ns/op на голой
	// карте, при нулевых аллокациях в обоих случаях. Цена растёт с числом
	// партиций, а полезной работы в ней нет — карта после Start не меняется.
	//
	// Схема ровно та же, что у optsCache (otel.go): copy-on-write под
	// мьютексом на записи, atomic.Pointer на чтении. Цена — полная копия карты
	// на каждый AddHandler, но AddHandler вызывается считанные разы за жизнь
	// процесса и никогда с пути сообщения.
	//
	// Нулевой указатель — валидное состояние «обработчиков нет»: чтение из
	// nil-карты в Go законно, поэтому консьюмер, собранный литералом структуры
	// мимо конструктора, отвечает ErrNoHandlers на Start, а не паникует
	// разыменованием на пути сообщения.
	handlers atomic.Pointer[map[string]ConsumerHandler]

	// handlersMu защищает копирование карты при вставке. Читателям он не
	// нужен: снимок после публикации никто не меняет.
	handlersMu sync.Mutex

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

	// state — состояние жизненного цикла, см. consumerState. Читается Start и
	// AddHandler, взводится в consumerClosed в начале остановки: Start обязан
	// узнать о ней раньше, чем опубликует созданного клиента.
	state atomic.Int32

	// stopOnce делает Stop одновременно однократным и блокирующим: второй
	// вызывающий ждёт первого и получает тот же результат, а не nil «уже
	// останавливаемся». Без этого отмена контекста Start, которая теперь и
	// сама зовёт Stop, отнимала бы у явного Stop любую возможность узнать,
	// закончилось завершение или ещё идёт.
	stopOnce sync.Once
	stopErr  error
}

// NewConsumer создаёт консьюмера.
//
// Соединения здесь не устанавливаются и горутины не запускаются: набор топиков
// известен только после AddHandler, а franz-go требует его при создании
// клиента, поэтому сам клиент создаётся в Start. Конструктор проверяет
// конфигурацию, готовит логгер, метрики и репортер паник.
func NewConsumer(config Config) (*Consumer, error) {
	const op = "creating consumer"

	// Не оборачивается: у агрегата валидации Unwrap() []error, и fmt.Errorf
	// подменил бы его на Unwrap() error — документированный разбор списка
	// перестал бы работать ровно там, где он нужен.
	if err := config.validateConsumer(); err != nil {
		return nil, err
	}

	logger := config.logger("kafka_consumer").With(slog.String("group", config.Consumer.Group))

	metrics, err := newConsumerMetrics(otel.Meter(instrumentationName, meterOptions()...))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	lifeCtx, lifeCancel := context.WithCancel(context.Background())

	return &Consumer{
		config:       config,
		logger:       logger,
		telemetry:    newTelemetry(config.ClientID, config.Consumer.Group),
		metrics:      metrics,
		panics:       panicReporter{logger: logger, panics: metrics.panics, onPanic: config.OnPanic},
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
//
// На запущенном консьюмере возвращает ErrConsumerStarted, на остановленном —
// ErrConsumerClosed. Второе не «то же самое, но позже»: регистрация на
// остановленном консьюмере бесполезна навсегда, и раньше она молча
// завершалась успехом, создавая впечатление рабочей подписки.
//
// Отказ ничего не меняет: набор обработчиков публикуется целиком, поэтому ни
// дубликат, ни отказ по жизненному циклу не подменяют уже зарегистрированный
// обработчик и не добавляют половину нового.
func (c *Consumer) AddHandler(topic string, handler ConsumerHandler, mws ...ConsumerMiddleware) error {
	if topic == "" {
		return fmt.Errorf("adding handler: %w", ErrEmptyTopic)
	}

	// Сравнение с nil ловит только нетипизированный nil; типизированный
	// nil-указатель в интерфейсе пройдёт, но его паника станет
	// ErrHandlerPanic — то есть штатной ошибкой обработки.
	if handler == nil {
		return fmt.Errorf("adding handler for topic %q: %w", topic, ErrNilHandler)
	}

	if err := c.loadState().lifecycleErr(); err != nil {
		return fmt.Errorf("adding handler for topic %q: %w", topic, err)
	}

	c.handlersMu.Lock()
	defer c.handlersMu.Unlock()

	current := c.loadHandlers()
	if _, exists := current[topic]; exists {
		return fmt.Errorf("adding handler for topic %q: %w", topic, ErrDuplicateHandler)
	}

	// Карта копируется целиком, а не правится на месте: опубликованный снимок
	// уже могут читать воркеры, и запись в него была бы гонкой. Копия платится
	// один раз за регистрацию — на пути сообщения AddHandler не случается
	// вовсе.
	//
	// Публикация идёт последней строкой метода: до неё все отказы уже
	// случились, и отвергнутая регистрация снимок не трогает.
	next := make(map[string]ConsumerHandler, len(current)+1)
	maps.Copy(next, current)

	// Цепочка middleware собирается один раз при регистрации, а не на каждое
	// сообщение: аллокации замыканий на горячем пути ничего не дают.
	next[topic] = Chain(handler, mws...)

	// По той же причине здесь прогреваются опции метрик: набор атрибутов
	// топика известен целиком уже сейчас, и строить его заново на каждое
	// сообщение незачем. Прогрев стоит до публикации снимка: обратный порядок
	// оставлял бы окно «обработчик виден, опций его топика ещё нет», и
	// закрывал бы его сейчас только запрет AddHandler после Start — то есть
	// гарантия снаружи этой функции, а не в ней.
	c.opts.warm(topic, consumerStatuses...)

	c.handlers.Store(&next)

	return nil
}

// loadHandlers возвращает текущий снимок карты обработчиков.
//
// nil-указатель разворачивается в nil-карту, а не в панику: чтение из
// nil-карты законно и означает ровно то, что нужно, — обработчиков нет.
func (c *Consumer) loadHandlers() map[string]ConsumerHandler {
	if snapshot := c.handlers.Load(); snapshot != nil {
		return *snapshot
	}

	return nil
}

// loadState возвращает текущее состояние жизненного цикла.
func (c *Consumer) loadState() consumerState {
	return consumerState(c.state.Load())
}

// abortStart возвращает консьюмера в consumerIdle после неуспешного Start,
// чтобы можно было исправить конфигурацию и повторить запуск.
//
// CAS, а не Store: параллельный Stop мог уже перевести консьюмера в
// consumerClosed, и безусловная запись воскресила бы остановленного.
func (c *Consumer) abortStart() {
	c.state.CompareAndSwap(int32(consumerRunning), int32(consumerIdle))
}

// Start создаёт клиента Kafka и запускает цикл опроса. Не блокирует.
//
// Отмена ctx запускает ровно тот же путь, что и Stop, — с дренажем очередей и
// финальным коммитом. Разница только в том, что ошибку завершения при этом
// некому вернуть: она уходит в лог. Предпочтительнее явный Stop.
//
// Повторный вызов уже запущенного консьюмера возвращает ErrConsumerStarted.
// Консьюмер, прошедший Stop, не перезапускается: Start вернёт ErrConsumerClosed,
// и отличить это от «уже запущен» можно через errors.Is.
func (c *Consumer) Start(ctx context.Context) error {
	const op = "starting consumer"

	// Гонку двух Start разрешает CAS; проигравший узнаёт причину отказа из
	// состояния. К моменту чтения оно могло вернуться в consumerIdle — если
	// победивший откатился по ошибке конфигурации, — и тогда честнее всего
	// сказать «занято»: запуска не было именно из-за встречного вызова.
	if !c.state.CompareAndSwap(int32(consumerIdle), int32(consumerRunning)) {
		if err := c.loadState().lifecycleErr(); err != nil {
			return err
		}

		return ErrConsumerStarted
	}

	// Состояние откатывается на каждом неуспешном пути: иначе после отказа
	// Start исправить конфигурацию и повторить запуск было бы нельзя.
	topics := c.topics()
	if len(topics) == 0 {
		c.abortStart()

		return ErrNoHandlers
	}

	// Быстрый путь: Stop уже прошёл, создавать клиента незачем. Гарантию даёт
	// не эта проверка, а повторная — под c.mu, рядом с публикацией клиента.
	if c.loadState() == consumerClosed {
		return ErrConsumerClosed
	}

	opts, err := c.config.consumerOpts(c.logger, topics, rebalanceCallbacks{
		assigned: c.onPartitionsAssigned,
		revoked:  c.onPartitionsRevoked,
		lost:     c.onPartitionsLost,
	})
	if err != nil {
		c.abortStart()

		return fmt.Errorf("%s: %w", op, err)
	}

	// Хуки kotel питают трейсер: именно OnFetchRecordBuffered кладёт в
	// rec.Context извлечённый из заголовков W3C trace context, на котором
	// потом строится спан обработки.
	opts = append(opts, kgo.WithHooks(c.telemetry.hooks...))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		c.abortStart()

		return fmt.Errorf("%s: creating kafka client: %w", op, err)
	}

	pollCtx, pollCancel := context.WithCancel(c.lifeCtx)

	// Проверка состояния и публикация клиента — одна критическая секция, и это
	// обязательное условие, а не аккуратность. Stop переводит консьюмера в
	// consumerClosed до захвата c.mu, поэтому разнести их значит открыть окно, в
	// котором Stop видит c.client == nil, уходит по ранней ветке и оставляет уже
	// созданного клиента — присоединившегося к группе, с живым heartbeat — без
	// единого владельца, способного его закрыть. Типовой триггер — SIGTERM во
	// время старта пода.
	c.mu.Lock()
	if c.loadState() == consumerClosed {
		c.mu.Unlock()
		pollCancel()
		client.CloseAllowingRebalance()

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
func (c *Consumer) watchContext(ctx context.Context) {
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
func (c *Consumer) topics() []string {
	return slices.Sorted(maps.Keys(c.loadHandlers()))
}

// handler ищет обработчик топика. Горячий путь: вызывается на каждое
// сообщение из горутины воркера партиции, замков не берёт — см. поле handlers.
func (c *Consumer) handler(topic string) (ConsumerHandler, bool) {
	h, ok := c.loadHandlers()[topic]

	return h, ok
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
func (c *Consumer) Stop() error {
	c.stopOnce.Do(func() { c.stopErr = c.shutdown() })

	return c.stopErr
}

// shutdown — тело остановки, выполняемое ровно один раз.
func (c *Consumer) shutdown() error {
	// Терминальное состояние взводится до захвата c.mu — на этом порядке
	// держится защита от «осиротевшего» клиента в Start, см. комментарий там.
	c.state.Store(int32(consumerClosed))

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
		// Здесь лог остаётся: ErrPollLoopStuck сообщает вызывающему факт, но не
		// его последствие — открытого клиента, который переживёт Stop. Это
		// состояние процесса, а не результат вызова, и место ему в журнале.
		c.metrics.drainTimeouts.Add(context.WithoutCancel(c.lifeCtx), 1,
			metric.WithAttributes(attribute.String("phase", phasePollLoop)))
		c.logger.Error("Poll loop is still running after hard cancellation; " +
			"leaving the kafka client open to avoid racing it")

		return ErrPollLoopStuck
	}

	// Дренаж здесь не дублирует колбэк отзыва, хотя выглядит именно так:
	// CloseAllowingRebalance ниже выводит участника из группы, уход зовёт
	// onPartitionsRevoked, и тот делает тот же обход воркеров и тот же
	// CommitMarkedOffsets. Отличий два, и оба существенны.
	//
	// Первое — кому достаётся отказ. Коммит из колбэка отзыва вернуть некому,
	// его провал остаётся строкой в логе; здесь дренаж стоит ПЕРЕД финальным
	// коммитом, поэтому оффсет дообработанного сообщения уезжает тем самым
	// коммитом, ошибку которого Stop отдаёт вызывающему.
	//
	// Второе — бюджет. Колбэк отзыва ограничен RebalanceTimeout и про
	// GracefulTimeout не знает, а именно GracefulTimeout попадает в
	// terminationGracePeriodSeconds. Конфигурация с GracefulTimeout меньше
	// RebalanceTimeout (умолчания — 3m и 1m, но бюджет завершения уменьшают
	// куда чаще, чем таймаут ребаланса) дренировалась бы дольше обещанного и
	// получала бы SIGKILL до финального коммита.
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
		// Только счётчик и возврат, без записи в лог: ошибка уходит
		// вызывающему, и он её залогирует — а пакет, залогировав сам, удваивал
		// бы событие в журнале. Метрика нужна ровно потому, что дисциплины
		// «читать возврат Stop» у типового defer нет, и без неё проваленный
		// финальный коммит не существовал бы ни для одного алерта.
		c.metrics.commitErrors.Add(context.WithoutCancel(c.lifeCtx), 1,
			metric.WithAttributes(attribute.String("phase", phaseShutdown)))

		err = fmt.Errorf("%w: %w", ErrCommitFailed, commitErr)
	}

	client.CloseAllowingRebalance()
	c.lifeCancel()

	c.logger.Info("Kafka consumer shutdown completed")

	return err
}
