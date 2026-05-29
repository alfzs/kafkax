package kafkax

import (
	"context"
	"errors"
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
	lastActivity time.Time
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
		return err
	}

	worker.updateActivity()

	select {
	case worker.messageChan <- msg:
		select {
		case <-ctx.Done():
			return errors.New("context canceled")
		case err := <-resultChan:
			return err
		case <-time.After(p.messageTimeout):
			return errors.New("result wait timeout")
		}
	case <-ctx.Done():
		return errors.New("context canceled while queuing")
	case <-time.After(p.messageTimeout):
		return errors.New("enqueue timeout")
	}
}

func (p *KafkaProducer) getOrCreateWorker(tenantID uuid.UUID, logger *slog.Logger) (*tenantWorker, error) {
	// fast path
	p.workerLock.RLock()
	worker, ok := p.tenantPools[tenantID]
	p.workerLock.RUnlock()
	if ok {
		return worker, nil
	}

	// slow path — double-checked locking
	p.workerLock.Lock()
	defer p.workerLock.Unlock()

	if worker, ok = p.tenantPools[tenantID]; ok {
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

	logger.Info("Created new worker for tenant")
	return worker, nil
}

// runWorker обрабатывает исходящие сообщения для конкретного тенанта.
//
// Воркер намеренно НЕ удаляет себя из tenantPools при завершении.
// Удалением занимается только cleanupInactiveWorkers (по TTL) и Close (при shutdown).
// Это предотвращает race condition, когда cleanup уже удалил воркер из мапы,
// а горутина воркера при своём defer снова лезет в мапу и случайно удаляет
// только что созданный новый воркер для того же tenantID.
func (p *KafkaProducer) runWorker(tenantID uuid.UUID, worker *tenantWorker, logger *slog.Logger) {
	defer func() {
		worker.cancel()
		p.wg.Done()
		logger.Debug("Worker terminated", slog.String("tenant", tenantID.String()))
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
					slog.String("topic", msg.Topic))
			case <-time.After(msg.Timeout):
				logger.Warn("Message result not delivered - timeout",
					slog.String("topic", msg.Topic))
			}

		case <-worker.ctx.Done():
			return
		}
	}
}

// produce отправляет сообщение в Kafka и ожидает подтверждения доставки.
//
// deliveryChan намеренно не закрывается: confluent-kafka-go пишет в него
// асинхронно из внутреннего event loop'а. Закрытие канала до получения события
// вызвало бы панику в библиотеке при попытке записи в закрытый канал.
// Канал буферизован на 1 элемент, поэтому утечки горутин нет — библиотека
// запишет событие и продолжит работу независимо от того, читаем ли мы его.
func (p *KafkaProducer) produce(msg Message, logger *slog.Logger) error {
	deliveryChan := make(chan kafka.Event, 1)

	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &msg.Topic,
			Partition: kafka.PartitionAny,
		},
		Key:   msg.Key,
		Value: msg.Value,
	}, deliveryChan)

	if err != nil {
		return fmt.Errorf("produce error: %w", err)
	}

	select {
	case e := <-deliveryChan:
		m, ok := e.(*kafka.Message)
		if !ok {
			return fmt.Errorf("unexpected event type %T", e)
		}
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("delivery error: %w", m.TopicPartition.Error)
		}
		logger.Debug("Message delivered successfully",
			slog.String("topic", msg.Topic),
			slog.Int("partition", int(m.TopicPartition.Partition)),
			slog.Int64("offset", int64(m.TopicPartition.Offset)))
		return nil
	case <-msg.Ctx.Done():
		return msg.Ctx.Err()
	case <-time.After(msg.Timeout):
		return fmt.Errorf("produce timeout after %v", msg.Timeout)
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

// cleanupInactiveWorkers — единственное место, где воркеры удаляются из tenantPools.
// Воркеры не удаляют себя сами (см. комментарий в runWorker).
func (p *KafkaProducer) cleanupInactiveWorkers() {
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
				slog.Time("last_active", lastActive))
		}
	}
}

func (p *KafkaProducer) Close() {
	if !p.stopping.CompareAndSwap(false, true) {
		p.logger.Info("Kafka producer already in stopping state")
		return
	}

	p.logger.Info("Starting kafka producer shutdown")

	p.cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("All workers finished")
	case <-time.After(p.flushTimeout):
		p.logger.Warn("Shutdown timed out, forcing flush")
	}

	remaining := p.producer.Flush(int(p.flushTimeout.Milliseconds()))
	if remaining > 0 {
		p.logger.Warn("Messages remaining in queue after flush",
			slog.Int("count", remaining))
	}

	p.producer.Close()
	p.logger.Info("Kafka producer shutdown completed")
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
