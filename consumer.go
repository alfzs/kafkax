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

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type consumerHandler interface {
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
	inactiveWorkerTTL     time.Duration
	cleanupWorkerInterval time.Duration
	messageReadTimeout    time.Duration
	readErrorBackoff      time.Duration
	messageChanBuffer     int
	stopCleanup           chan struct{}
}

func NewKafkaConsumer(config Config, logger *slog.Logger) (*KafkaConsumer, error) {
	op := "new_kafka_consumer"

	if logger == nil {
		return nil, fmt.Errorf("%s: logger is nil", op)
	}

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":                     strings.Join(config.Brokers, ","),
		"client.id":                             config.ClientID,
		"group.id":                              config.Consumer.Group,
		"auto.offset.reset":                     config.Consumer.InitialOffset,
		"fetch.min.bytes":                       config.Consumer.MinBytes,
		"fetch.max.bytes":                       config.Consumer.MaxBytes,
		"enable.auto.commit":                    config.Consumer.EnableAutoCommit,
		"socket.timeout.ms":                     int(config.Consumer.SocketTimeout.Milliseconds()),
		"session.timeout.ms":                    int(config.Consumer.SessionTimeout.Milliseconds()),
		"heartbeat.interval.ms":                 int(config.Consumer.HeartbeatInterval.Milliseconds()),
		"max.poll.interval.ms":                  int(config.Consumer.MaxPollInterval.Milliseconds()),
		"isolation.level":                       config.Consumer.IsolationLevel,
		"security.protocol":                     config.SecurityProtocol,
		"sasl.mechanisms":                       config.SASL.Mechanism,
		"sasl.username":                         config.SASL.Username,
		"sasl.password":                         config.SASL.Password,
		"ssl.endpoint.identification.algorithm": config.TLS.IdentificationAlgorithm,
		"ssl.ca.location":                       config.TLS.CaCertPath,
		"ssl.certificate.location":              config.TLS.ClientCertPath,
		"ssl.key.location":                      config.TLS.ClientKeyPath,
	}

	consumer, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("%s: kafka consumer failed init: %w", op, err)
	}

	return &KafkaConsumer{
		consumer:              consumer,
		config:                config,
		logger:                logger.With(slog.String("component", "kafka_consumer")),
		handlers:              make(map[string]consumerHandler),
		workers:               make(map[workerKey]*partitionWorker),
		inactiveWorkerTTL:     config.Consumer.InactiveWorkerTTL,
		cleanupWorkerInterval: config.Consumer.CleanupWorkerInterval,
		messageReadTimeout:    config.Consumer.ReadTimeout,
		readErrorBackoff:      max(config.Consumer.ReadTimeout, time.Second),
		messageChanBuffer:     config.Consumer.MessageQueueSize,
		stopCleanup:           make(chan struct{}),
	}, nil
}

func (c *KafkaConsumer) AddHandler(topic string, handler consumerHandler) error {
	c.handlersMu.Lock()
	defer c.handlersMu.Unlock()

	if _, ok := c.handlers[topic]; ok {
		return fmt.Errorf("handler for topic %s already registered", topic)
	}

	c.handlers[topic] = handler
	return nil
}

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

func (c *KafkaConsumer) Start(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)

	c.handlersMu.RLock()
	noHandlers := len(c.handlers) == 0
	c.handlersMu.RUnlock()

	if noHandlers {
		return fmt.Errorf("no kafka handlers registered")
	}

	c.wg.Add(1)
	go c.runConsumerLoop()

	c.wg.Add(1)
	go c.runCleanupLoop()

	c.logger.Info("Kafka consumer started")
	return nil
}

func (c *KafkaConsumer) runConsumerLoop() {
	defer c.wg.Done()
	defer c.cancel()

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info("Consumer loop stopped")
			return
		default:
			msg, err := c.consumer.ReadMessage(c.messageReadTimeout)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				// Не-timeout ошибка (например, недоступны все брокеры) может
				// возвращаться немедленно и без бэкоффа превратит цикл в
				// busy-loop с заливкой логов. Ждём перед следующей попыткой,
				// но не дольше readErrorBackoff и с учётом отмены контекста.
				c.logger.Error("Failed to read message", slog.Any("error", err))
				select {
				case <-time.After(c.readErrorBackoff):
				case <-c.ctx.Done():
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
// Запись в messageChan блокирующая (без отдельного enqueue-таймаута): пока
// сообщения обрабатываются и коммитятся строго по порядку, дроп сообщения
// "из середины" с продолжением чтения дальше привёл бы к тому, что offset
// дропнутого сообщения окажется меньше уже закоммиченного следующего — оно
// будет потеряно навсегда. Если воркер не успевает, обратное давление должно
// притормозить consumer loop целиком (это ограничено max.poll.interval.ms).
func (c *KafkaConsumer) processMessage(msg *kafka.Message) {
	partition := msg.TopicPartition.Partition
	topic := *msg.TopicPartition.Topic

	log := c.logger.With(
		slog.Int("partition", int(partition)),
		slog.String("topic", topic))

	worker, err := c.getOrCreateWorker(topic, partition, log)
	if err != nil {
		// Консьюмер в процессе остановки: сообщение не коммитим и не
		// обрабатываем, оно будет переобработано после перезапуска.
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

	// Stop() тоже берёт workersMu перед cancel(), поэтому если stopping уже
	// true, wg.Add ниже гарантированно не выполнится после старта wg.Wait().
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

	log.Info("Created new partition worker")
	return worker, nil
}

// runPartitionWorker обрабатывает сообщения для конкретной партиции.
//
// При завершении (штатном или по панике) воркер удаляется из мапы немедленно,
// а не ждёт следующего прохода cleanupInactiveWorkers. Это гарантирует, что
// после паники новые сообщения получат свежий воркер, а не попытаются писать
// в мёртвый канал.
//
// При штатной остановке (worker.ctx.Done) канал дочитывается до конца (drain),
// чтобы не терять сообщения, уже поставленные в очередь. Drain ограничен по
// времени отдельным контекстом (а не Background), чтобы не держать обработку
// дольше GracefulTimeout, после которого Stop() закроет c.consumer и
// CommitMessage из drain'а станет невалидным.
func (c *KafkaConsumer) runPartitionWorker(key workerKey, worker *partitionWorker) {
	defer c.wg.Done()
	defer worker.cancel()
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("Partition worker panic",
				slog.Any("recover", r),
				slog.String("stack", string(debug.Stack())),
				slog.String("topic", key.topic),
				slog.Int("partition", int(key.partition)))
		}
		// Удаляем воркер из мапы независимо от причины завершения.
		// Cleanup-loop удаляет только по TTL, поэтому мёртвый воркер мог бы
		// жить в мапе до следующего тика — здесь закрываем эту дыру.
		c.workersMu.Lock()
		// Проверяем, что в мапе именно наш воркер, а не уже новый
		// (теоретически возможно при очень быстром пересоздании).
		if current, ok := c.workers[key]; ok && current == worker {
			delete(c.workers, key)
		}
		c.workersMu.Unlock()
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
				default:
					return
				}
			}
		}
	}
}

// handleMessage вызывает зарегистрированный handler и коммитит оффсет при успехе.
// Принимает ctx явно, чтобы при drain-фазе можно было передать ограниченный
// по времени контекст вместо отменённого worker.ctx.
func (c *KafkaConsumer) handleMessage(ctx context.Context, msg *kafka.Message) {
	topic := *msg.TopicPartition.Topic

	c.handlersMu.RLock()
	handler, ok := c.handlers[topic]
	c.handlersMu.RUnlock()

	if !ok {
		c.logger.Error("No handler for topic", slog.String("topic", topic))
		return
	}

	if err := handler.ProcessMessage(ctx, msg.Value); err != nil {
		c.logger.Error("Failed to process message",
			slog.String("topic", topic),
			slog.Any("error", err))
		return
	}

	if _, err := c.consumer.CommitMessage(msg); err != nil {
		c.logger.Error("Failed to commit message",
			slog.String("topic", topic),
			slog.Any("error", err))
	}
}

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

// cleanupInactiveWorkers — единственное место, где воркеры удаляются из мапы
// по TTL. Воркер с inFlight > 0 не трогаем: значит, прямо сейчас есть
// processMessage, который получил ссылку на воркер и либо ещё не обновил
// lastActivity, либо пишет в messageChan — отмена контекста сейчас могла бы
// увести воркер в drain раньше, чем сообщение попадёт в канал.
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

func (c *KafkaConsumer) Stop() {
	if !c.stopping.CompareAndSwap(false, true) {
		c.logger.Info("Already in stopping state")
		return
	}

	c.logger.Info("Starting kafka consumer shutdown")

	// Сначала останавливаем cleanup, затем отменяем контекст воркеров.
	// Порядок важен: если сначала cancel(), cleanup-loop может попытаться
	// удалить воркеры, которые уже завершаются — безвредно, но избыточно.
	close(c.stopCleanup)

	// cancel() под workersMu: getOrCreateWorker проверяет stopping под тем же
	// локом перед wg.Add, поэтому к моменту wg.Wait() ниже новые wg.Add из
	// getOrCreateWorker уже невозможны.
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
		c.logger.Warn("Shutdown timed out, forcing close")
	}

	// consumer.Close() вызывается после того, как runConsumerLoop завершил работу
	// (он завершается по ctx.Done() до этой точки), поэтому ReadMessage уже
	// не вызывается. Drain-фаза в runPartitionWorker ограничена тем же
	// GracefulTimeout через drainCtx, поэтому к этому моменту CommitMessage
	// из drain'а тоже не должен выполняться — закрытие безопасно.
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
