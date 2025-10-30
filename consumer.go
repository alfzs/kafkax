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

type partitionWorker struct {
	messageChan  chan *kafka.Message
	ctx          context.Context
	cancel       context.CancelFunc
	lastActivity time.Time
	mu           sync.Mutex
}

type consumerHandler interface {
	ProcessMessage(ctx context.Context, data []byte) error
}

type KafkaConsumer struct {
	consumer              *kafka.Consumer
	config                Config
	logger                *slog.Logger
	handlers              map[string]consumerHandler
	handlersMu            sync.RWMutex
	workers               map[int32]*partitionWorker
	workersMu             sync.RWMutex
	wg                    sync.WaitGroup
	ctx                   context.Context
	cancel                context.CancelFunc
	stopping              atomic.Bool
	inactiveWorkerTTL     time.Duration
	cleanupWorkerInterval time.Duration
	messageReadTimeout    time.Duration
	maxEnqueueTimeout     time.Duration
	messageChanBuffer     int
	stopCleanup           chan struct{}
}

// INFO: NewKafkaConsumer клиент для работы с Кафка
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
		workers:               make(map[int32]*partitionWorker),
		inactiveWorkerTTL:     config.Consumer.InactiveWorkerTTL,
		cleanupWorkerInterval: config.Consumer.CleanupWorkerInterval,
		messageReadTimeout:    config.Consumer.ReadTimeout,
		maxEnqueueTimeout:     config.Consumer.MaxEnqueueTimeout,
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
	c.handlersMu.Lock()
	defer c.handlersMu.Unlock()

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
	if len(c.handlers) == 0 {
		c.handlersMu.RUnlock()
		return fmt.Errorf("no kafka handlers registered")
	}
	c.handlersMu.RUnlock()

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
				c.logger.Error("Failed to read message", slog.Any("error", err))
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

func (c *KafkaConsumer) processMessage(msg *kafka.Message) {
	partition := msg.TopicPartition.Partition
	topic := *msg.TopicPartition.Topic

	c.workersMu.Lock()
	defer c.workersMu.Unlock()

	worker, ok := c.workers[partition]
	if !ok {
		workerCtx, cancel := context.WithCancel(c.ctx)
		worker = &partitionWorker{
			messageChan:  make(chan *kafka.Message, c.messageChanBuffer),
			ctx:          workerCtx,
			cancel:       cancel,
			lastActivity: time.Now(),
		}
		c.workers[partition] = worker

		c.wg.Add(1)
		go c.runPartitionWorker(partition, worker)

		c.logger.Info("Created new partition worker",
			slog.Int("partition", int(partition)),
			slog.String("topic", topic))
	}

	select {
	case worker.messageChan <- msg:
		worker.updateActivity()
	case <-time.After(c.maxEnqueueTimeout):
		c.logger.Warn("Failed to enqueue message",
			slog.Int("partition", int(partition)),
			slog.String("topic", topic))
	case <-worker.ctx.Done():
		c.logger.Warn("Worker context done while enqueueing",
			slog.Int("partition", int(partition)))
	}
}

func (c *KafkaConsumer) runPartitionWorker(partition int32, worker *partitionWorker) {
	defer func() {
		worker.cancel()
		c.wg.Done()
		if r := recover(); r != nil {
			c.logger.Error("Partition worker panic",
				slog.Any("recover", r),
				slog.String("stack", string(debug.Stack())),
				slog.Int("partition", int(partition)))
		}
	}()

	for {
		select {
		case msg, ok := <-worker.messageChan:
			if !ok {
				return
			}
			worker.updateActivity()
			c.handleMessage(worker, msg)
		case <-worker.ctx.Done():
			return
		}
	}
}

func (c *KafkaConsumer) handleMessage(worker *partitionWorker, msg *kafka.Message) {
	topic := *msg.TopicPartition.Topic

	c.handlersMu.RLock()
	handler, ok := c.handlers[topic]
	c.handlersMu.RUnlock()

	if !ok {
		c.logger.Error("No handler for topic", slog.String("topic", topic))
		return
	}

	if err := handler.ProcessMessage(worker.ctx, msg.Value); err != nil {
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

func (c *KafkaConsumer) cleanupInactiveWorkers() {
	c.workersMu.Lock()
	defer c.workersMu.Unlock()

	now := time.Now()
	inactiveSince := now.Add(-c.inactiveWorkerTTL)

	for partition, worker := range c.workers {
		lastActive := worker.getLastActivity()
		if lastActive.Before(inactiveSince) {
			worker.cancel()
			delete(c.workers, partition)
			c.logger.Info("Removed inactive worker",
				slog.Int("partition", int(partition)),
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

	close(c.stopCleanup)
	c.cancel()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.logger.Info("Kafka consumer fully stopped")
	case <-time.After(c.config.GracefulTimeout):
		c.logger.Warn("Shutdown timed out")
	}

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
