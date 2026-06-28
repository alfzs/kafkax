package kafkax

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// consumerHandler — интерфейс обработчика сообщений Kafka.
// Реализуется пользователем библиотеки и регистрируется через AddHandler.
// ctx содержит OTel-span (SpanKind=Consumer) и может быть отменён при остановке консьюмера.
type consumerHandler interface {
	// ProcessMessage вызывается для каждого сообщения из топика.
	// При возврате ошибки вызов будет повторён до HandlerMaxRetries раз.
	// Если сообщение не может быть обработано, следует вернуть ошибку —
	// после исчерпания попыток offset будет закоммичен и сообщение пропущено.
	ProcessMessage(ctx context.Context, data []byte) error
}

type partitionWorker struct {
	messageChan  chan *kafka.Message
	ctx          context.Context
	cancel       context.CancelFunc
	lastActivity time.Time
	inFlight     int64 // atomic: число processMessage, держащих ссылку на этот воркер
	mu           sync.Mutex
}

type workerKey struct {
	topic     string
	partition int32
}

type consumerMetrics struct {
	processed      metric.Int64Counter
	failed         metric.Int64Counter
	retried        metric.Int64Counter
	processingTime metric.Float64Histogram
	commitErrors   metric.Int64Counter
	workersActive  metric.Int64UpDownCounter
}

// KafkaConsumer — консьюмер Kafka с изоляцией обработки по партициям.
// Для каждой активной партиции поддерживается отдельный воркер, что обеспечивает
// параллельную обработку сообщений из разных партиций при сохранении порядка внутри одной.
// Безопасен для конкурентного использования из нескольких горутин.
type KafkaConsumer struct {
	consumer              *kafka.Consumer
	config                Config
	logger                *slog.Logger
	handlers              map[string]consumerHandler
	handlersMu            sync.RWMutex
	workers               map[workerKey]*partitionWorker
	workersMu             sync.RWMutex
	wg                    sync.WaitGroup
	ctx                   context.Context
	cancel                context.CancelFunc
	stopping              atomic.Bool
	started               atomic.Bool
	inactiveWorkerTTL     time.Duration
	cleanupWorkerInterval time.Duration
	messageReadTimeout    time.Duration
	readErrorBackoff      time.Duration
	retryDelay            time.Duration
	messageChanBuffer     int
	stopCleanup           chan struct{}
	tracer                trace.Tracer
	propagator            propagation.TextMapPropagator
	metrics               consumerMetrics
}

// NewKafkaConsumer создаёт консьюмер Kafka.
//
// Инициализирует соединение с брокером, OTel-инструменты и внутренний контекст.
// Горутины запускаются только при вызове Start; NewKafkaConsumer безопасен сам по себе.
// Возвращает ошибку при невалидной конфигурации или невозможности инициализировать клиент Kafka.
func NewKafkaConsumer(config Config) (*KafkaConsumer, error) {
	op := "new_kafka_consumer"

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers":                     strings.Join(config.Brokers, ","),
		"client.id":                             config.ClientID,
		"group.id":                              config.Consumer.Group,
		"auto.offset.reset":                     config.Consumer.InitialOffset,
		"fetch.min.bytes":                       config.Consumer.MinBytes,
		"fetch.max.bytes":                       config.Consumer.MaxBytes,
		"fetch.wait.max.ms":                     int(config.Consumer.MaxWait.Milliseconds()),
		"enable.auto.commit":                    config.Consumer.EnableAutoCommit,
		"socket.timeout.ms":                     int(config.Consumer.SocketTimeout.Milliseconds()),
		"session.timeout.ms":                    int(config.Consumer.SessionTimeout.Milliseconds()),
		"heartbeat.interval.ms":                 int(config.Consumer.HeartbeatInterval.Milliseconds()),
		"max.poll.interval.ms":                  int(config.Consumer.MaxPollInterval.Milliseconds()),
		"isolation.level":                       config.Consumer.IsolationLevel,
		"security.protocol":                     config.SecurityProtocol,
		"ssl.endpoint.identification.algorithm": config.TLS.endpointIdentAlgorithm(),
		"ssl.ca.location":                       config.TLS.CaCertPath,
		"ssl.certificate.location":              config.TLS.ClientCertPath,
		"ssl.key.location":                      config.TLS.ClientKeyPath,
	}
	// SASL параметры передаются только при соответствующем протоколе;
	// librdkafka запрещает пустое значение sasl.mechanisms.
	proto := strings.ToUpper(config.SecurityProtocol)
	if proto == "SASL_PLAINTEXT" || proto == "SASL_SSL" {
		kafkaConfig["sasl.mechanisms"] = config.SASL.Mechanism
		kafkaConfig["sasl.username"] = config.SASL.Username
		kafkaConfig["sasl.password"] = config.SASL.Password
	}

	consumer, err := kafka.NewConsumer(&kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("%s: kafka consumer failed init: %w", op, err)
	}

	// ctx/cancel инициализируются здесь, а не только в Start(),
	// чтобы Stop() не паниковал при вызове без предшествующего Start().
	ctx, cancel := context.WithCancel(context.Background())

	meter := otel.Meter("github.com/alfzs/kafkax/consumer")

	processed, _ := meter.Int64Counter("kafkax.consumer.messages.processed",
		metric.WithDescription("Total messages successfully processed and committed"))
	failed, _ := meter.Int64Counter("kafkax.consumer.messages.failed",
		metric.WithDescription("Total messages skipped after exhausting handler retries"))
	retried, _ := meter.Int64Counter("kafkax.consumer.messages.retried",
		metric.WithDescription("Total handler retry attempts"))
	procTime, _ := meter.Float64Histogram("kafkax.consumer.processing.duration",
		metric.WithDescription("Time spent in ProcessMessage handler"),
		metric.WithUnit("ms"))
	commitErrors, _ := meter.Int64Counter("kafkax.consumer.commit.errors",
		metric.WithDescription("Total failed CommitMessage calls"))
	workersActive, _ := meter.Int64UpDownCounter("kafkax.consumer.workers.active",
		metric.WithDescription("Number of active partition worker goroutines"))

	c := &KafkaConsumer{
		consumer:              consumer,
		config:                config,
		logger:                slog.Default().With(slog.String("component", "kafka_consumer"), slog.String("group", config.Consumer.Group)),
		handlers:              make(map[string]consumerHandler),
		workers:               make(map[workerKey]*partitionWorker),
		ctx:                   ctx,
		cancel:                cancel,
		inactiveWorkerTTL:     config.Consumer.InactiveWorkerTTL,
		cleanupWorkerInterval: config.Consumer.CleanupWorkerInterval,
		messageReadTimeout:    config.Consumer.ReadTimeout,
		readErrorBackoff:      config.Consumer.ReadErrorBackoff,
		retryDelay:            config.Consumer.HandlerRetryDelay,
		messageChanBuffer:     config.Consumer.MessageQueueSize,
		stopCleanup:           make(chan struct{}),
		tracer:                otel.Tracer("github.com/alfzs/kafkax/consumer"),
		propagator:            otel.GetTextMapPropagator(),
		metrics: consumerMetrics{
			processed:      processed,
			failed:         failed,
			retried:        retried,
			processingTime: procTime,
			commitErrors:   commitErrors,
			workersActive:  workersActive,
		},
	}

	// Observable gauge: глубина очередей по партициям.
	_, _ = meter.Int64ObservableGauge("kafkax.consumer.queue.depth",
		metric.WithDescription("Messages pending in partition worker queues"),
		metric.WithUnit("{message}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			c.workersMu.RLock()
			defer c.workersMu.RUnlock()
			for key, w := range c.workers {
				o.Observe(int64(len(w.messageChan)),
					metric.WithAttributes(
						attribute.String("topic", key.topic),
						attribute.Int("partition", int(key.partition))))
			}
			return nil
		}))

	return c, nil
}

// AddHandler регистрирует обработчик для указанного топика.
// Должен вызываться до Start. Повторная регистрация одного топика возвращает ошибку.
// Безопасен для конкурентного вызова.
func (c *KafkaConsumer) AddHandler(topic string, handler consumerHandler) error {
	c.handlersMu.Lock()
	defer c.handlersMu.Unlock()

	if _, ok := c.handlers[topic]; ok {
		return fmt.Errorf("handler for topic %s already registered", topic)
	}

	c.handlers[topic] = handler
	return nil
}

// SubscribeAll подписывает консьюмер на все топики, для которых зарегистрированы обработчики.
// Должен вызываться после AddHandler и до Start.
// Возвращает ошибку, если ни одного обработчика не зарегистрировано.
func (c *KafkaConsumer) SubscribeAll() error {
	c.handlersMu.RLock()
	defer c.handlersMu.RUnlock()

	topics := make([]string, 0, len(c.handlers))
	for t := range c.handlers {
		topics = append(topics, t)
	}

	if len(topics) == 0 {
		return fmt.Errorf("no topics to subscribe")
	}

	return c.consumer.SubscribeTopics(topics, nil)
}

// Start запускает consumer loop и фоновый сборщик неактивных воркеров.
//
// ctx используется как родительский контекст для всех воркеров: его отмена
// равносильна вызову Stop, но без graceful drain и вызова consumer.Close.
// Для управляемого завершения предпочтительнее явный вызов Stop.
//
// Идемпотентен через atomic.Bool: повторный вызов возвращает ошибку немедленно.
// Требует наличия хотя бы одного зарегистрированного обработчика.
func (c *KafkaConsumer) Start(ctx context.Context) error {
	if !c.started.CompareAndSwap(false, true) {
		return fmt.Errorf("consumer already started")
	}

	// Отменяем background-контекст из NewKafkaConsumer и подменяем его на
	// контекст с реальным родителем. Безопасно — горутин ещё нет.
	c.cancel()
	c.ctx, c.cancel = context.WithCancel(ctx)

	c.handlersMu.RLock()
	topics := make([]string, 0, len(c.handlers))
	for t := range c.handlers {
		topics = append(topics, t)
	}
	c.handlersMu.RUnlock()

	if len(topics) == 0 {
		return fmt.Errorf("no kafka handlers registered")
	}

	c.wg.Add(1)
	go c.runConsumerLoop()

	c.wg.Add(1)
	go c.runCleanupLoop()

	c.logger.Info("Kafka consumer started", slog.Any("topics", topics))
	return nil
}

// runConsumerLoop читает сообщения из Kafka и передаёт их в processMessage.
// Таймаут ReadTimeout предотвращает вечную блокировку — при ErrTimedOut цикл продолжается,
// что обеспечивает отзывчивость на ctx.Done при отсутствии новых сообщений.
func (c *KafkaConsumer) runConsumerLoop() {
	defer c.wg.Done()
	defer c.cancel()

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Debug("Consumer loop stopped")
			return
		default:
			msg, err := c.consumer.ReadMessage(c.messageReadTimeout)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				c.logger.Error("Failed to read message", slog.Any("error", err))
				backoffTimer := time.NewTimer(c.readErrorBackoff)
				select {
				case <-backoffTimer.C:
				case <-c.ctx.Done():
					backoffTimer.Stop()
				}
				continue
			}

			if msg == nil {
				c.logger.Error("Received nil message")
				continue
			}

			c.processMessage(msg)
		}
	}
}

// processMessage находит или создаёт воркер для партиции и передаёт сообщение.
// Лок берётся только на время поиска/создания воркера, а не на время записи в канал.
// Это предотвращает блокировку consumer loop'а под локом на время записи.
//
// Запись в messageChan блокирующая (без enqueue-таймаута): дроп сообщения
// "из середины" привёл бы к тому, что offset дропнутого окажется меньше
// закоммиченного следующего — сообщение потеряно навсегда. Обратное давление
// притормаживает consumer loop (ограничено max.poll.interval.ms).
func (c *KafkaConsumer) processMessage(msg *kafka.Message) {
	if msg.TopicPartition.Topic == nil {
		c.logger.Error("Received message with nil topic, skipping")
		return
	}
	partition := msg.TopicPartition.Partition
	topic := *msg.TopicPartition.Topic

	log := c.logger.With(
		slog.Int("partition", int(partition)),
		slog.String("topic", topic))

	worker, err := c.getOrCreateWorker(topic, partition, log)
	if err != nil {
		// Consumer в процессе остановки: сообщение не коммитим,
		// оно будет переобработано после перезапуска.
		log.Warn("Dropping message: consumer is shutting down", slog.Any("error", err))
		return
	}
	defer atomic.AddInt64(&worker.inFlight, -1)

	select {
	case worker.messageChan <- msg:
		worker.updateActivity()
	case <-worker.ctx.Done():
		log.Warn("Worker context done while enqueueing, message not committed")
	case <-c.ctx.Done():
	}
}

// getOrCreateWorker возвращает существующий воркер партиции или создаёт новый.
// Использует double-checked locking: сначала RLock (fast path), затем Lock (slow path).
// Атомарно инкрементирует inFlight, чтобы cleanup не уничтожил воркер между
// получением ссылки и записью в messageChan.
func (c *KafkaConsumer) getOrCreateWorker(topic string, partition int32, log *slog.Logger) (*partitionWorker, error) {
	key := workerKey{topic: topic, partition: partition}

	// fast path
	c.workersMu.RLock()
	worker, ok := c.workers[key]
	if ok {
		atomic.AddInt64(&worker.inFlight, 1)
	}
	c.workersMu.RUnlock()
	if ok {
		return worker, nil
	}

	// slow path — double-checked locking
	c.workersMu.Lock()
	defer c.workersMu.Unlock()

	// Stop() берёт workersMu перед cancel(), поэтому если stopping уже true,
	// wg.Add ниже гарантированно не выполнится после wg.Wait() в Stop().
	if c.stopping.Load() {
		return nil, fmt.Errorf("consumer is shutting down")
	}

	if worker, ok = c.workers[key]; ok {
		atomic.AddInt64(&worker.inFlight, 1)
		return worker, nil
	}

	workerCtx, cancel := context.WithCancel(c.ctx)
	worker = &partitionWorker{
		messageChan:  make(chan *kafka.Message, c.messageChanBuffer),
		ctx:          workerCtx,
		cancel:       cancel,
		lastActivity: time.Now(),
		inFlight:     1,
	}
	c.workers[key] = worker

	c.wg.Add(1)
	go c.runPartitionWorker(key, worker)

	c.metrics.workersActive.Add(context.Background(), 1)
	log.Info("Created new partition worker")
	return worker, nil
}

// runPartitionWorker обрабатывает сообщения для конкретной партиции.
//
// При завершении (штатном или по панике) воркер удаляется из мапы немедленно,
// гарантируя что после паники новые сообщения получат свежий воркер.
//
// При штатной остановке (worker.ctx.Done) канал дочитывается до конца (drain),
// ограниченный drainCtx с тем же GracefulTimeout, что используется в Stop().
// Это гарантирует, что CommitMessage не вызывается после consumer.Close().
func (c *KafkaConsumer) runPartitionWorker(key workerKey, worker *partitionWorker) {
	defer c.wg.Done()
	defer worker.cancel()
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("Partition worker panic",
				slog.Any("panic", r),
				slog.String("stack", string(debug.Stack())),
				slog.String("topic", key.topic),
				slog.Int("partition", int(key.partition)))
		}
		// Удаляем воркер из мапы немедленно — cleanup удаляет только по TTL,
		// поэтому мёртвый воркер иначе жил бы в мапе до следующего тика.
		c.workersMu.Lock()
		if current, ok := c.workers[key]; ok && current == worker {
			delete(c.workers, key)
		}
		c.workersMu.Unlock()
		c.metrics.workersActive.Add(context.Background(), -1)
	}()

	for {
		select {
		case msg, ok := <-worker.messageChan:
			if !ok {
				return
			}
			worker.updateActivity()
			c.handleMessage(worker.ctx, msg)
		case <-worker.ctx.Done():
			drainCtx, cancel := context.WithTimeout(context.Background(), c.config.GracefulTimeout)
			defer cancel()

			for {
				select {
				case msg, ok := <-worker.messageChan:
					if !ok {
						return
					}
					c.handleMessage(drainCtx, msg)
				// Прерываем drain если GracefulTimeout истёк: Stop() уже вызвал
				// consumer.Close() — дальнейший CommitMessage недопустим.
				case <-drainCtx.Done():
					return
				default:
					return
				}
			}
		}
	}
}

// handleMessage вызывает зарегистрированный handler с retry-логикой и коммитит
// offset при завершении (успех или исчерпание попыток). ctx передаётся явно:
// при drain используется ограниченный по времени drainCtx.
//
// При HandlerMaxRetries > 0 и исчерпании попыток сообщение пропускается (offset
// коммитится), чтобы яд-сообщение не блокировало партицию навсегда.
func (c *KafkaConsumer) handleMessage(ctx context.Context, msg *kafka.Message) {
	topic := *msg.TopicPartition.Topic

	// Извлекаем trace context из Kafka headers и создаём consumer-span.
	headers := msg.Headers
	extractCtx := c.propagator.Extract(ctx, newKafkaHeaderCarrier(&headers))
	ctx, span := c.tracer.Start(extractCtx, topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.operation.name", "process"),
			attribute.String("messaging.kafka.consumer.group", c.config.Consumer.Group),
			attribute.Int("messaging.kafka.partition", int(msg.TopicPartition.Partition)),
			attribute.Int64("messaging.kafka.offset", int64(msg.TopicPartition.Offset)),
		))
	defer span.End()

	c.handlersMu.RLock()
	handler, ok := c.handlers[topic]
	c.handlersMu.RUnlock()

	if !ok {
		c.logger.Error("No handler for topic", slog.String("topic", topic))
		return
	}

	topicAttr := metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("consumer_group", c.config.Consumer.Group))

	maxRetries := c.config.Consumer.HandlerMaxRetries
	start := time.Now()
	var handlerErr error

	for attempt := 1; ; attempt++ {
		if err := handler.ProcessMessage(ctx, msg.Value); err != nil {
			handlerErr = err

			if maxRetries > 0 && attempt >= maxRetries {
				c.logger.Error("Skipping message after max retries",
					slog.String("topic", topic),
					slog.Int("partition", int(msg.TopicPartition.Partition)),
					slog.Int64("offset", int64(msg.TopicPartition.Offset)),
					slog.Int("attempts", attempt),
					slog.Any("error", err))
				span.SetStatus(codes.Error, err.Error())
				c.metrics.failed.Add(ctx, 1, topicAttr)
				break
			}

			c.logger.Warn("Handler failed, retrying",
				slog.String("topic", topic),
				slog.Int("partition", int(msg.TopicPartition.Partition)),
				slog.Int64("offset", int64(msg.TopicPartition.Offset)),
				slog.Int("attempt", attempt),
				slog.Int("max_retries", maxRetries),
				slog.Any("error", err))
			c.metrics.retried.Add(ctx, 1, topicAttr)

			retryTimer := time.NewTimer(c.retryDelay)
			select {
			case <-retryTimer.C:
			case <-ctx.Done():
				retryTimer.Stop()
				// Контекст отменён во время паузы между попытками — не коммитим.
				// При перезапуске сообщение будет обработано заново.
				return
			}
			continue
		}

		handlerErr = nil
		break
	}

	c.metrics.processingTime.Record(ctx,
		float64(time.Since(start).Milliseconds()), topicAttr)

	if handlerErr == nil {
		c.metrics.processed.Add(ctx, 1, topicAttr)
	}

	// Коммитим offset в любом случае: при успехе — нормально, при провале
	// после max retries — пропускаем сообщение (poison pill protection).
	if _, err := c.consumer.CommitMessage(msg); err != nil {
		c.logger.Error("Failed to commit message",
			slog.String("topic", topic),
			slog.Int("partition", int(msg.TopicPartition.Partition)),
			slog.Int64("offset", int64(msg.TopicPartition.Offset)),
			slog.Any("error", err))
		c.metrics.commitErrors.Add(ctx, 1, topicAttr)
	}
}

// runCleanupLoop — фоновая горутина, периодически вызывающая cleanupInactiveWorkers.
// Завершается либо по ctx.Done (штатная остановка), либо по stopCleanup (вызов Stop).
func (c *KafkaConsumer) runCleanupLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.cleanupWorkerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanupInactiveWorkers()
		case <-c.ctx.Done():
			return
		case <-c.stopCleanup:
			return
		}
	}
}

// cleanupInactiveWorkers удаляет воркеры по TTL.
// Воркер с inFlight > 0 не трогаем: значит прямо сейчас есть processMessage,
// который получил ссылку на воркер и либо ещё не обновил lastActivity,
// либо пишет в messageChan — отмена контекста сейчас увела бы воркер в drain
// до записи сообщения.
func (c *KafkaConsumer) cleanupInactiveWorkers() {
	c.workersMu.Lock()
	defer c.workersMu.Unlock()

	now := time.Now()
	inactiveSince := now.Add(-c.inactiveWorkerTTL)

	for key, worker := range c.workers {
		if atomic.LoadInt64(&worker.inFlight) > 0 {
			continue
		}

		lastActive := worker.getLastActivity()
		if lastActive.Before(inactiveSince) {
			worker.cancel()
			delete(c.workers, key)
			c.logger.Info("Removed inactive worker",
				slog.String("topic", key.topic),
				slog.Int("partition", int(key.partition)),
				slog.Time("last_active", lastActive))
		}
	}
}

// Stop выполняет graceful shutdown консьюмера:
//  1. Останавливает фоновый сборщик воркеров.
//  2. Отменяет контекст всех воркеров партиций, запуская drain их очередей.
//  3. Ожидает завершения всех горутин до GracefulTimeout.
//  4. Вызывает consumer.Close для закрытия соединения с брокером.
//
// Безопасен для повторного вызова: последующие вызовы логируют предупреждение и возвращаются немедленно.
func (c *KafkaConsumer) Stop() {
	if !c.stopping.CompareAndSwap(false, true) {
		c.logger.Warn("Already in stopping state")
		return
	}

	c.logger.Info("Starting kafka consumer shutdown")

	// Сначала останавливаем cleanup, затем отменяем контекст воркеров.
	// Порядок важен: если сначала cancel(), cleanup-loop может попытаться
	// удалить воркеры, которые уже завершаются — безвредно, но избыточно.
	close(c.stopCleanup)

	// cancel() под workersMu: getOrCreateWorker проверяет stopping под тем же
	// локом перед wg.Add, поэтому к моменту wg.Wait() новые wg.Add невозможны.
	c.workersMu.Lock()
	c.cancel()
	c.workersMu.Unlock()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.logger.Info("Kafka consumer fully stopped")
	case <-time.After(c.config.GracefulTimeout):
		c.logger.Warn("Shutdown timed out, forcing close",
			slog.String("timeout", c.config.GracefulTimeout.String()))
	}

	// consumer.Close() безопасен: runConsumerLoop завершился по ctx.Done(),
	// drain каждого воркера ограничен drainCtx с тем же GracefulTimeout.
	if err := c.consumer.Close(); err != nil {
		c.logger.Error("Failed to close consumer", slog.Any("error", err))
	}

	c.logger.Info("Kafka consumer shutdown completed")
}

func (w *partitionWorker) updateActivity() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastActivity = time.Now()
}

func (w *partitionWorker) getLastActivity() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastActivity
}
