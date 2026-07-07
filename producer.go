package kafkax

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
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

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

type tenantWorker struct {
	// inFlight — первое поле умышленно: atomic.AddInt64/LoadInt64 требуют
	// 64-битного выравнивания, которое рантайм Go гарантирует только для
	// первого слова аллоцированной структуры на 32-битных платформах.
	inFlight    int64 // atomic: число SendMessage, держащих ссылку на этот воркер
	messageChan chan message
	//nolint:containedctx // lifecycle-контекст воркера (аналог BaseContext), не запросный — см. docs/context-audit.md
	ctx          context.Context
	cancel       context.CancelFunc
	lastActivity time.Time
	mu           sync.Mutex
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

// message — внутренняя единица очереди воркера тенанта, не часть публичного API.
type message struct {
	//nolint:containedctx // message — элемент очереди воркера, передаваемый через канал; см. docs/context-audit.md
	Ctx      context.Context
	TenantID uuid.UUID
	Topic    string
	Key      []byte
	Value    []byte
	Headers  Headers
	Result   chan error
	Timeout  time.Duration
}

type producerMetrics struct {
	sent           metric.Int64Counter
	failed         metric.Int64Counter
	messageLatency metric.Float64Histogram
	workersActive  metric.Int64UpDownCounter
}

// KafkaProducer — продюсер Kafka с изоляцией по тенантам.
// Для каждого уникального TenantID поддерживается отдельный воркер с буферным каналом,
// что обеспечивает независимую обработку очередей разных тенантов.
// Безопасен для конкурентного использования из нескольких горутин.
type KafkaProducer struct {
	producer    *kafka.Producer
	config      Config
	logger      *slog.Logger
	tenantPools map[uuid.UUID]*tenantWorker
	workerLock  sync.RWMutex
	wg          sync.WaitGroup
	//nolint:containedctx // lifecycle-контекст компонента (аналог BaseContext), не запросный — см. docs/context-audit.md
	ctx                   context.Context
	cancel                context.CancelFunc
	closed                chan struct{}
	isStopping            atomic.Bool
	inactiveWorkerTTL     time.Duration
	cleanupWorkerInterval time.Duration
	flushTimeout          time.Duration
	messageTimeout        time.Duration
	messageChanBuffer     int
	tracer                trace.Tracer
	propagator            propagation.TextMapPropagator
	metrics               producerMetrics
}

// buildProducerKafkaConfig транслирует Config в kafka.ConfigMap для librdkafka.
func buildProducerKafkaConfig(config Config) kafka.ConfigMap {
	kafkaConfig := kafka.ConfigMap{
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
		"ssl.endpoint.identification.algorithm": config.TLS.endpointIdentAlgorithm(),
		"ssl.ca.location":                       config.TLS.CaCertPath,
		"ssl.certificate.location":              config.TLS.ClientCertPath,
		"ssl.key.location":                      config.TLS.ClientKeyPath,
	}
	// SASL параметры передаются только при соответствующем протоколе;
	// librdkafka запрещает пустое значение sasl.mechanisms.
	proto := strings.ToUpper(config.SecurityProtocol)
	if proto == SecurityProtocolSASLPlaintext || proto == SecurityProtocolSASLSSL {
		kafkaConfig["sasl.mechanisms"] = config.SASL.Mechanism
		kafkaConfig["sasl.username"] = config.SASL.Username
		kafkaConfig["sasl.password"] = config.SASL.Password
	}

	return kafkaConfig
}

// newProducerMetrics регистрирует OTel-инструменты продюсера в переданном meter.
func newProducerMetrics(meter metric.Meter) producerMetrics {
	sent, _ := meter.Int64Counter("kafkax.producer.messages.sent",
		metric.WithDescription("Total messages successfully delivered to Kafka"))
	failed, _ := meter.Int64Counter("kafkax.producer.messages.failed",
		metric.WithDescription("Total messages that failed delivery"))
	latency, _ := meter.Float64Histogram("kafkax.producer.message.duration",
		metric.WithDescription("End-to-end produce latency from Produce() call to delivery ack"),
		metric.WithUnit("ms"))
	workersActive, _ := meter.Int64UpDownCounter("kafkax.producer.workers.active",
		metric.WithDescription("Number of active tenant worker goroutines"))

	return producerMetrics{
		sent:           sent,
		failed:         failed,
		messageLatency: latency,
		workersActive:  workersActive,
	}
}

// NewKafkaProducer создаёт и запускает продюсер Kafka.
//
// ctx используется как родительский контекст продюсера: его отмена эквивалентна
// вызову Close (запускается та же горутина graceful shutdown). Для управляемого
// завершения по-прежнему предпочтительнее явный вызов Close — так вызывающий
// код может дождаться его завершения синхронно, а не полагаться на фоновую
// горутину-наблюдателя.
//
// Инициализирует OTel-инструменты (счётчики, гистограммы, gauge) и запускает
// фоновую горутину сборщика неактивных воркеров. Возвращает ошибку при невалидной
// конфигурации или невозможности подключиться к брокеру.
func NewKafkaProducer(ctx context.Context, config Config) (*KafkaProducer, error) {
	const op = "new_kafka_producer"

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	kafkaConfig := buildProducerKafkaConfig(config)

	producer, err := kafka.NewProducer(&kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	lifecycleCtx, cancel := context.WithCancel(ctx)

	meter := otel.Meter("github.com/alfzs/kafkax/producer")

	p := &KafkaProducer{
		producer:              producer,
		config:                config,
		logger:                slog.Default().With(slog.String("component", "kafka_producer")),
		tenantPools:           make(map[uuid.UUID]*tenantWorker),
		ctx:                   lifecycleCtx,
		cancel:                cancel,
		closed:                make(chan struct{}),
		inactiveWorkerTTL:     config.Producer.InactiveWorkerTTL,
		cleanupWorkerInterval: config.Producer.CleanupWorkerInterval,
		flushTimeout:          config.Producer.FlushTimeout,
		messageTimeout:        config.Producer.MessageTimeout,
		messageChanBuffer:     config.Producer.MessageQueueSize,
		tracer:                otel.Tracer("github.com/alfzs/kafkax/producer"),
		propagator:            otel.GetTextMapPropagator(),
		metrics:               newProducerMetrics(meter),
	}

	// Observable gauge: суммарная глубина очередей всех воркеров.
	// Захватывает p по указателю — безопасно, т.к. p живёт дольше любого тика метрик.
	// После Close() tenantPools пуст, callback просто ничего не наблюдает.
	//
	// tenant_id как label не используется намеренно: число тенантов неограниченно,
	// а unbounded label взрывает кардинальность метрики в бэкенде. Поэтому глубина
	// суммируется по всем тенантам в одно значение.
	if _, err := meter.Int64ObservableGauge("kafkax.producer.queue.depth",
		metric.WithDescription("Total messages pending across all tenant worker queues"),
		metric.WithUnit("{message}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			p.workerLock.RLock()
			defer p.workerLock.RUnlock()

			var total int64
			for _, w := range p.tenantPools {
				total += int64(len(w.messageChan))
			}

			o.Observe(total)

			return nil
		})); err != nil {
		producer.Close()
		return nil, fmt.Errorf("%s: registering queue depth gauge: %w", op, err)
	}

	p.wg.Add(1)
	go p.manageWorkers()

	// Наблюдатель за отменой ctx, переданного вызывающим кодом: доводит
	// отмену до полноценного Close (Flush + закрытие соединения с брокером).
	// Не входит в p.wg — иначе wg.Wait() в Close() ждал бы эту же горутину,
	// которая сама вызывает Close() (self-deadlock/ложный timeout).
	// Слушает p.closed, а не p.ctx, чтобы не запускать повторный Close()
	// каждый раз, когда explicit Close() сам отменяет p.ctx.
	go func() {
		select {
		case <-ctx.Done():
			p.Close()
		case <-p.closed:
		}
	}()

	return p, nil
}

// PublishRequest описывает сообщение для отправки через SendMessage.
type PublishRequest struct {
	TenantID uuid.UUID
	Topic    string
	Key      []byte
	Value    []byte
	Headers  Headers
}

// SendMessage отправляет сообщение в указанный топик Kafka и блокируется до
// получения delivery ack от брокера или истечения Config.Producer.MessageTimeout.
//
// ctx может содержать активный OTel-span: продюсер создаст дочерний span
// (SpanKind=Producer) и инжектирует trace context в Kafka headers, что позволяет
// консьюмеру восстановить цепочку трассировки.
//
// Возможные ошибки:
//   - "producer is shutting down" — Close уже вызван
//   - "context canceled" / "context canceled while queuing" — ctx отменён
//   - "timeout queuing message to worker" — воркер переполнен
//   - "timeout waiting for delivery ack" — брокер не ответил за MessageTimeout
//   - "tenant worker unavailable" — воркер завершился во время постановки в очередь
func (p *KafkaProducer) SendMessage(ctx context.Context, req PublishRequest) error {
	if p.isStopping.Load() {
		return errors.New("producer is shutting down")
	}

	if err := validateHeaders(req.Headers); err != nil {
		return err
	}

	log := p.logger.With(slog.String("tenant_id", req.TenantID.String()))
	if sc := trace.SpanFromContext(ctx).SpanContext(); sc.IsValid() {
		log = log.With(slog.String("trace_id", sc.TraceID().String()))
	}

	resultChan := make(chan error, 1)

	msg := message{
		Ctx:      ctx,
		TenantID: req.TenantID,
		Topic:    req.Topic,
		Key:      req.Key,
		Value:    req.Value,
		Headers:  req.Headers,
		Result:   resultChan,
		Timeout:  p.messageTimeout,
	}

	worker, err := p.getOrCreateWorker(req.TenantID, log)
	if err != nil {
		return err
	}
	// inFlight держит воркер "занятым" на всё время SendMessage, чтобы cleanup
	// не отменил его между получением ссылки и записью в messageChan.
	defer atomic.AddInt64(&worker.inFlight, -1)

	worker.updateActivity()

	enqueueTimer := time.NewTimer(p.messageTimeout)
	defer enqueueTimer.Stop()

	select {
	case worker.messageChan <- msg:
		resultTimer := time.NewTimer(p.messageTimeout)
		defer resultTimer.Stop()

		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled: %w", ctx.Err())
		case err := <-resultChan:
			return err
		case <-resultTimer.C:
			return errors.New("timeout waiting for delivery ack")
		}
	// Воркер уже отменён (cleanup или shutdown): возвращаем ошибку немедленно,
	// не пишем в канал — drain проверяет inFlight перед выходом.
	case <-worker.ctx.Done():
		return errors.New("tenant worker unavailable")
	case <-ctx.Done():
		return fmt.Errorf("context canceled while queuing: %w", ctx.Err())
	case <-enqueueTimer.C:
		return errors.New("timeout queuing message to worker")
	}
}

// getOrCreateWorker возвращает существующий воркер тенанта или создаёт новый.
// Использует double-checked locking: сначала RLock (fast path), затем Lock (slow path).
// Атомарно инкрементирует inFlight, чтобы cleanup не уничтожил воркер между
// получением ссылки и записью в messageChan.
func (p *KafkaProducer) getOrCreateWorker(tenantID uuid.UUID, logger *slog.Logger) (*tenantWorker, error) {
	// fast path
	p.workerLock.RLock()

	worker, ok := p.tenantPools[tenantID]
	if ok {
		atomic.AddInt64(&worker.inFlight, 1)
	}

	p.workerLock.RUnlock()

	if ok {
		return worker, nil
	}

	// slow path — double-checked locking
	p.workerLock.Lock()
	defer p.workerLock.Unlock()

	// Close() берёт workerLock перед cancel(), поэтому если isStopping уже true,
	// wg.Add ниже гарантированно не выполнится после wg.Wait() в Close().
	if p.isStopping.Load() {
		return nil, errors.New("producer is shutting down")
	}

	if worker, ok = p.tenantPools[tenantID]; ok {
		atomic.AddInt64(&worker.inFlight, 1)
		return worker, nil
	}

	workerCtx, cancel := context.WithCancel(p.ctx)
	worker = &tenantWorker{
		messageChan:  make(chan message, p.messageChanBuffer),
		ctx:          workerCtx,
		cancel:       cancel,
		lastActivity: time.Now(),
		inFlight:     1,
	}

	p.tenantPools[tenantID] = worker

	p.wg.Add(1)
	go p.runWorker(tenantID, worker, logger)

	p.metrics.workersActive.Add(context.Background(), 1)
	logger.Info("Created new worker for tenant")

	return worker, nil
}

// runWorker обрабатывает исходящие сообщения для конкретного тенанта.
//
// Воркер намеренно НЕ удаляет себя из tenantPools при завершении.
// Удалением занимается только cleanupInactiveWorkers (TTL) и Close (shutdown).
// Это предотвращает race condition при быстром пересоздании воркера для того
// же tenantID.
func (p *KafkaProducer) runWorker(tenantID uuid.UUID, worker *tenantWorker, logger *slog.Logger) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("Worker panic",
				slog.Any("panic", r),
				slog.String("stack", string(debug.Stack())),
				slog.String("tenant_id", tenantID.String()))
		}

		worker.cancel()
		p.metrics.workersActive.Add(context.Background(), -1)
		p.wg.Done()
		logger.Debug("Worker terminated", slog.String("tenant_id", tenantID.String()))
	}()

	for {
		select {
		case msg := <-worker.messageChan:
			worker.updateActivity()
			p.handleMessage(msg, logger)

		case <-worker.ctx.Done():
			// Drain: сообщения, записанные в messageChan до отмены контекста,
			// должны быть обработаны — вызывающий ждёт resultChan.
			//
			// inFlight check: SendMessage мог пройти fast-path getOrCreateWorker
			// (inFlight++) до отмены контекста, но ещё не записал в messageChan.
			// Spin-wait гарантирует, что мы не выйдем раньше этой записи.
			for {
				select {
				case msg := <-worker.messageChan:
					p.handleMessage(msg, logger)
				default:
					if atomic.LoadInt64(&worker.inFlight) > 0 {
						runtime.Gosched()
						continue
					}

					return
				}
			}
		}
	}
}

// handleMessage вызывает produce и доставляет результат в msg.Result.
// Если вызывающая сторона не читает resultChan (контекст отменён или таймаут),
// результат отбрасывается с предупреждением — produce уже завершился.
func (p *KafkaProducer) handleMessage(msg message, logger *slog.Logger) {
	err := p.produce(msg)

	timer := time.NewTimer(msg.Timeout)
	defer timer.Stop()

	select {
	case msg.Result <- err:
	case <-msg.Ctx.Done():
		logger.Warn("Message result not delivered - context canceled",
			slog.String("topic", msg.Topic))
	case <-timer.C:
		logger.Warn("Message result not delivered - timeout",
			slog.String("topic", msg.Topic))
	}
}

// produce отправляет сообщение в Kafka и ожидает подтверждения доставки.
//
// Trace context из msg.Ctx инжектируется в Kafka headers — consumer может
// извлечь его и создать дочерний span для сквозной трассировки.
//
// deliveryChan намеренно не закрывается: confluent-kafka-go пишет в него
// асинхронно из внутреннего event loop'а. Закрытие канала вызвало бы панику
// в библиотеке при попытке записи в закрытый канал. Буфер на 1 элемент
// исключает утечку горутин — библиотека запишет событие независимо от того,
// читаем ли мы его.
//
// p.ctx.Done() в select позволяет Close() быстро прервать ожидание delivery
// и завершить воркеры до вызова producer.Close(). Сообщение при этом могло
// уже попасть в очередь librdkafka и будет доставлено через Flush() —
// возврат ошибки не гарантирует недоставку.
func (p *KafkaProducer) produce(msg message) (err error) {
	headers := toKafkaHeaders(msg.Headers)

	// Создаём producer-span и инжектируем trace context в headers.
	ctx, span := p.tracer.Start(msg.Ctx, msg.Topic+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", msg.Topic),
			attribute.String("messaging.operation.name", "publish"),
		))
	defer span.End()

	topicAttr := metric.WithAttributes(attribute.String("topic", msg.Topic))

	// Гистограмма latency пишется для любого исхода (не только success),
	// иначе отправки с ошибкой/таймаутом выпадают из распределения и p99
	// выглядит здоровым даже при росте доли отказов.
	start := time.Now()

	defer func() {
		status := "success"
		if err != nil {
			status = "error"
		}

		p.metrics.messageLatency.Record(ctx, float64(time.Since(start).Milliseconds()),
			metric.WithAttributes(attribute.String("topic", msg.Topic), attribute.String("status", status)))
	}()

	p.propagator.Inject(ctx, newKafkaHeaderCarrier(&headers))

	deliveryChan := make(chan kafka.Event, 1)

	if produceErr := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &msg.Topic,
			Partition: kafka.PartitionAny,
		},
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	}, deliveryChan); produceErr != nil {
		span.RecordError(produceErr)
		span.SetStatus(codes.Error, produceErr.Error())
		p.metrics.failed.Add(ctx, 1, topicAttr)

		err = fmt.Errorf("produce error: %w", produceErr)

		return err
	}

	timer := time.NewTimer(msg.Timeout)
	defer timer.Stop()

	select {
	case e := <-deliveryChan:
		m, ok := e.(*kafka.Message)
		if !ok {
			unexpectedErr := fmt.Errorf("unexpected event type %T", e)
			span.RecordError(unexpectedErr)
			span.SetStatus(codes.Error, unexpectedErr.Error())
			p.metrics.failed.Add(ctx, 1, topicAttr)

			err = unexpectedErr

			return err
		}

		if m.TopicPartition.Error != nil {
			span.RecordError(m.TopicPartition.Error)
			span.SetStatus(codes.Error, m.TopicPartition.Error.Error())
			p.metrics.failed.Add(ctx, 1, topicAttr)

			err = fmt.Errorf("delivery error: %w", m.TopicPartition.Error)

			return err
		}

		p.metrics.sent.Add(ctx, 1, topicAttr)
		span.SetAttributes(
			attribute.Int("messaging.kafka.partition", int(m.TopicPartition.Partition)),
			attribute.Int64("messaging.kafka.offset", int64(m.TopicPartition.Offset)),
		)

		return nil
	case <-p.ctx.Done():
		// Нормальное завершение при shutdown — не помечаем span как ошибку.
		err = errors.New("producer is shutting down")
		return err
	case <-msg.Ctx.Done():
		span.RecordError(msg.Ctx.Err())
		span.SetStatus(codes.Error, msg.Ctx.Err().Error())
		err = msg.Ctx.Err()

		return err
	case <-timer.C:
		timeoutErr := fmt.Errorf("produce timeout after %v", msg.Timeout)
		span.RecordError(timeoutErr)
		span.SetStatus(codes.Error, timeoutErr.Error())
		err = timeoutErr

		return err
	}
}

// manageWorkers — фоновая горутина, периодически вызывающая cleanupInactiveWorkers.
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
//
// Воркер с inFlight > 0 не трогаем: значит прямо сейчас есть SendMessage,
// который получил ссылку на воркер и ещё не записал сообщение в messageChan.
func (p *KafkaProducer) cleanupInactiveWorkers() {
	p.workerLock.Lock()
	defer p.workerLock.Unlock()

	now := time.Now()
	inactiveSince := now.Add(-p.inactiveWorkerTTL)

	for tenantID, worker := range p.tenantPools {
		if atomic.LoadInt64(&worker.inFlight) > 0 {
			continue
		}

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

// Close выполняет graceful shutdown продюсера:
//  1. Запрещает новые вызовы SendMessage и getOrCreateWorker.
//  2. Ожидает завершения всех воркеров (drain очередей) до FlushTimeout.
//  3. Вызывает Flush для доставки сообщений, оставшихся в очереди librdkafka.
//
// Безопасен для повторного вызова: последующие вызовы логируют предупреждение и возвращаются немедленно.
func (p *KafkaProducer) Close() {
	if !p.isStopping.CompareAndSwap(false, true) {
		p.logger.Warn("Kafka producer already in stopping state")
		return
	}
	// Сигнализирует наблюдателю за ctx (см. NewKafkaProducer), что Close уже
	// в процессе — второй вызов Close ему не нужен.
	close(p.closed)

	p.logger.Info("Starting kafka producer shutdown")

	// cancel() под workerLock: getOrCreateWorker проверяет isStopping под тем же
	// локом перед wg.Add, поэтому к моменту wg.Wait() новые wg.Add невозможны.
	// p.ctx.Done() также прерывает ожидание delivery в produce(), позволяя
	// воркерам завершиться до истечения flushTimeout.
	p.workerLock.Lock()
	p.cancel()
	p.workerLock.Unlock()

	done := make(chan struct{})

	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("All workers finished")
	case <-time.After(p.flushTimeout):
		p.logger.Warn("Shutdown timed out, forcing flush",
			slog.String("timeout", p.flushTimeout.String()))
	}

	remaining := p.producer.Flush(int(p.flushTimeout.Milliseconds()))
	if remaining > 0 {
		p.logger.Warn("Messages remaining in queue after flush",
			slog.Int("count", remaining))
	}

	p.producer.Close()
	p.logger.Info("Kafka producer shutdown completed")
}
