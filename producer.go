package kafkax

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alfzs/tracing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

type tenantWorker struct {
	messageChan  chan Message
	ctx          context.Context
	cancel       context.CancelFunc
	lastActivity time.Time // Время последнего использования
	mu           sync.Mutex
}

type Message struct {
	Ctx      context.Context
	TenantID uuid.UUID
	Topic    string
	Key      []byte
	Value    []byte
	Result   chan error
	Timeout  time.Duration
}

type KafkaProducer struct {
	producer              *kafka.Producer
	config                Config
	logger                *slog.Logger
	tenantPools           map[uuid.UUID]*tenantWorker
	workerLock            sync.RWMutex
	wg                    sync.WaitGroup
	ctx                   context.Context
	cancel                context.CancelFunc
	stopping              atomic.Bool
	inactiveWorkerTTL     time.Duration
	cleanupWorkerInterval time.Duration
	flushTimeout          time.Duration
	messageTimeout        time.Duration
	messageChanBuffer     int
}

// INFO: NewKafkaProducer клиент для работы с Кафка
func NewKafkaProducer(ctx context.Context, config Config, logger *slog.Logger) (*KafkaProducer, error) {
	const op = "new_kafka_producer"

	if logger == nil {
		return nil, fmt.Errorf("%s: logger is nil", op)
	}

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":                     strings.Join(config.Brokers, ","),
		"client.id":                             config.ClientID,
		"acks":                                  config.Producer.RequiredAcks,
		"retries":                               config.Producer.MaxRetries,
		"request.timeout.ms":                    int(config.Producer.AckTimeout.Milliseconds()),
		"retry.backoff.ms":                      int(config.Producer.RetryBackoff.Milliseconds()),
		"enable.idempotence":                    config.Producer.EnableIdempotence,
		"max.in.flight.requests.per.connection": config.Producer.MaxInflight,
		"linger.ms":                             int(config.Producer.Linger.Milliseconds()),
		"batch.num.messages":                    config.Producer.BatchSize,
		"batch.size":                            config.Producer.BatchBytes,
		"compression.type":                      config.Producer.CompressionType,
		"queue.buffering.max.ms":                int(config.Producer.BatchTimeout.Milliseconds()),
		"security.protocol":                     config.SecurityProtocol,
		"sasl.mechanisms":                       config.SASL.Mechanism,
		"sasl.username":                         config.SASL.Username,
		"sasl.password":                         config.SASL.Password,
		"ssl.endpoint.identification.algorithm": config.TLS.IdentificationAlgorithm,
		"ssl.ca.location":                       config.TLS.CaCertPath,
		"ssl.certificate.location":              config.TLS.ClientCertPath,
		"ssl.key.location":                      config.TLS.ClientKeyPath,
	}

	producer, err := kafka.NewProducer(kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	ctx, cancel := context.WithCancel(ctx)

	p := &KafkaProducer{
		producer:              producer,
		config:                config,
		logger:                logger.With(slog.String("component", "kafka_producer")),
		tenantPools:           make(map[uuid.UUID]*tenantWorker),
		ctx:                   ctx,
		cancel:                cancel,
		inactiveWorkerTTL:     config.Producer.InactiveWorkerTTL,
		cleanupWorkerInterval: config.Producer.CleanupWorkerInterval,
		flushTimeout:          config.Producer.FlushTimeout,
		messageTimeout:        config.Producer.AckTimeout,
		messageChanBuffer:     config.Producer.MessageQueueSize,
	}

	p.wg.Add(1)
	go p.manageWorkers()

	return p, nil
}

func (p *KafkaProducer) SendMessage(ctx context.Context, tenantID uuid.UUID, topic string, key, value []byte) error {
	op := "send_message"

	traceID := tracing.GetTraceID(ctx)
	log := p.logger.With(
		slog.String("trace_id", traceID),
		slog.String("tenant_id", tenantID.String()))

	resultChan := make(chan error, 1)

	msg := Message{
		Ctx:      ctx,
		TenantID: tenantID,
		Topic:    topic,
		Key:      key,
		Value:    value,
		Result:   resultChan,
		Timeout:  p.messageTimeout,
	}

	worker, err := p.getOrCreateWorker(tenantID, log)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	worker.updateActivity()

	select {
	case worker.messageChan <- msg:
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: context canceled", op)
		case err := <-resultChan:
			return err
		case <-time.After(p.messageTimeout):
			return fmt.Errorf("%s: result wait timeout", op)
		}
	case <-ctx.Done():
		return fmt.Errorf("%s: context canceled while queuing", op)
	case <-time.After(p.messageTimeout):
		return fmt.Errorf("%s: enqueue timeout", op)
	}
}

func (p *KafkaProducer) getOrCreateWorker(tenantID uuid.UUID, logger *slog.Logger) (*tenantWorker, error) {
	op := "get_or_create_worker"

	p.workerLock.RLock()
	worker, ok := p.tenantPools[tenantID]
	p.workerLock.RUnlock()

	if ok {
		return worker, nil
	}

	p.workerLock.Lock()
	defer p.workerLock.Unlock()

	if worker, ok := p.tenantPools[tenantID]; ok {
		return worker, nil
	}

	workerCtx, cancel := context.WithCancel(p.ctx)
	worker = &tenantWorker{
		messageChan:  make(chan Message, p.messageChanBuffer),
		ctx:          workerCtx,
		cancel:       cancel,
		lastActivity: time.Now(),
	}

	p.tenantPools[tenantID] = worker
	p.wg.Add(1)
	go p.runWorker(tenantID, worker, logger)

	logger.Info("Created new worker for tenant", slog.String("op", op))
	return worker, nil
}

func (p *KafkaProducer) runWorker(tenantID uuid.UUID, worker *tenantWorker, logger *slog.Logger) {
	op := "run_worker"

	defer func() {
		worker.cancel()
		p.workerLock.Lock()
		delete(p.tenantPools, tenantID)
		p.workerLock.Unlock()
		p.wg.Done()
		logger.Debug("Worker terminated",
			slog.String("tenant", tenantID.String()),
			slog.String("op", op))
	}()

	for {
		select {
		case msg := <-worker.messageChan:
			worker.updateActivity()
			err := p.produce(msg, logger)

			select {
			case msg.Result <- err:
			case <-msg.Ctx.Done():
				logger.Warn("Message result not delivered - context canceled",
					slog.String("topic", msg.Topic),
					slog.String("op", op))
			case <-time.After(msg.Timeout):
				logger.Warn("Message result not delivered - timeout",
					slog.String("topic", msg.Topic),
					slog.String("op", op))
			}

		case <-worker.ctx.Done():
			return
		}
	}
}

func (p *KafkaProducer) produce(msg Message, logger *slog.Logger) error {
	op := "produce"

	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)

	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &msg.Topic,
			Partition: kafka.PartitionAny,
		},
		Key:   msg.Key,
		Value: msg.Value,
	}, deliveryChan)

	if err != nil {
		return fmt.Errorf("%s: produce error: %w", op, err)
	}

	select {
	case e := <-deliveryChan:
		m, ok := e.(*kafka.Message)
		if !ok {
			return fmt.Errorf("%s: unexpected event type %T", op, e)
		}
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("%s: delivery error: %w", op, m.TopicPartition.Error)
		}
		logger.Debug("Message delivered successfully",
			slog.String("topic", msg.Topic),
			slog.Int("partition", int(m.TopicPartition.Partition)),
			slog.Int64("offset", int64(m.TopicPartition.Offset)),
			slog.String("op", op))
		return nil
	case <-msg.Ctx.Done():
		return msg.Ctx.Err()
	case <-time.After(msg.Timeout):
		return fmt.Errorf("%s: produce timeout after %v", op, msg.Timeout)
	}
}

func (p *KafkaProducer) manageWorkers() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.cleanupWorkerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.cleanupInactiveWorkers()
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *KafkaProducer) cleanupInactiveWorkers() {
	op := "cleanup_inactive_workers"

	p.workerLock.Lock()
	defer p.workerLock.Unlock()

	now := time.Now()
	inactiveSince := now.Add(-p.inactiveWorkerTTL)

	for tenantID, worker := range p.tenantPools {
		lastActive := worker.getLastActivity()
		if lastActive.Before(inactiveSince) {
			worker.cancel()
			delete(p.tenantPools, tenantID)
			p.logger.Info("Removed inactive worker",
				slog.String("tenant", tenantID.String()),
				slog.Time("last_active", lastActive),
				slog.String("op", op))
		}
	}
}

func (p *KafkaProducer) Close() {
	op := "close"

	if !p.stopping.CompareAndSwap(false, true) {
		p.logger.Info("Kafka producer already in stopping state", slog.String("op", op))
		return
	}

	p.logger.Info("Starting kafka producer shutdown", slog.String("op", op))

	p.cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("All workers finished", slog.String("op", op))
	case <-time.After(p.flushTimeout):
		p.logger.Warn("Shutdown timed out", slog.String("op", op))
	}

	remaining := p.producer.Flush(int(p.flushTimeout.Milliseconds()))
	if remaining > 0 {
		p.logger.Warn("Messages remaining in queue after flush",
			slog.Int("count", remaining),
			slog.String("op", op))
	}

	p.producer.Close()
	p.logger.Info("Kafka producer shutdown completed", slog.String("op", op))
}

func (w *tenantWorker) updateActivity() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastActivity = time.Now()
}

func (w *tenantWorker) getLastActivity() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastActivity
}
