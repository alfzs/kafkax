package kafkax

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"go.opentelemetry.io/otel"
)

// Общая тестовая инфраструктура пакета.
//
// Брокер здесь настоящий, но внутрипроцессный: kfake реализует протокол Kafka
// поверх net.Listener на localhost. Это снимает деление тестов на «unit» и
// «integration» — путь сообщения проверяется целиком, включая коммит оффсетов
// и ребаланс, без Docker и без пропуска тестов на машине без брокера.
const (
	testTopic    = "kafkax-test-topic"
	testClientID = "kafkax-test"
	testGroup    = "kafkax-test-group"
)

// unreachableBroker — адрес, на котором заведомо никто не слушает. Для тестов,
// которым брокер не нужен: клиент franz-go подключается лениво, поэтому
// конструктор и валидация конфигурации отрабатывают без сети.
const unreachableBroker = "127.0.0.1:1"

// newFakeCluster поднимает брокер kfake с уже созданными топиками и возвращает
// его адреса.
//
// Пороги сессии опущены до минимума: умолчания Kafka (6s) сделали бы каждый
// тест ребаланса шестисекундным, а проверяется в них логика колбэков, а не
// длительность таймаутов брокера.
func newFakeCluster(t *testing.T, partitions int32, topics ...string) []string {
	t.Helper()

	_, addrs := newFakeClusterHandle(t, partitions, topics...)

	return addrs
}

// newFakeClusterHandle — то же самое, но отдаёт и сам кластер. Нужен тестам,
// которые гасят брокер посреди сценария: обрыв связи посреди работы — это
// отдельный класс отказов, и подделать его подменой конфигурации нельзя.
//
// Close у kfake идемпотентен, поэтому тест закрывает кластер сам, а
// зарегистрированный здесь Cleanup остаётся страховкой на случай раннего
// t.Fatal.
func newFakeClusterHandle(t *testing.T, partitions int32, topics ...string) (*kfake.Cluster, []string) {
	t.Helper()

	if len(topics) == 0 {
		topics = []string{testTopic}
	}

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(partitions, topics...),
		kfake.GroupMinSessionTimeout(100*time.Millisecond),
		kfake.GroupMaxSessionTimeout(time.Minute),
	)
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}

	t.Cleanup(cluster.Close)

	return cluster, cluster.ListenAddrs()
}

// testLogger пишет логи библиотеки в журнал теста: при падении они видны, при
// успехе — нет. Уровень Info отсекает отладочный поток самого franz-go.
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()

	return slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// testConfig — конфигурация с короткими таймаутами, указывающая на brokers.
// Без аргументов даёт конфигурацию с недоступным брокером.
func testConfig(t *testing.T, brokers ...string) Config {
	t.Helper()

	if len(brokers) == 0 {
		brokers = []string{unreachableBroker}
	}

	return Config{
		Brokers:         brokers,
		ClientID:        testClientID,
		GracefulTimeout: 5 * time.Second,
		DialTimeout:     2 * time.Second,
		Logger:          testLogger(t),
		Producer: ProducerConfig{
			RequiredAcks:       -1,
			EnableIdempotence:  true,
			MaxInflight:        5,
			MaxRetries:         3,
			AckTimeout:         2 * time.Second,
			RetryBackoff:       50 * time.Millisecond,
			Linger:             0,
			BatchBytes:         1 << 20,
			CompressionType:    "none",
			MaxBufferedRecords: 1000,
			MessageTimeout:     3 * time.Second,
			FlushTimeout:       3 * time.Second,
		},
		Consumer: ConsumerConfig{
			Group:               testGroup,
			InitialOffset:       "earliest",
			MinBytes:            1,
			MaxBytes:            1 << 20,
			MaxPartitionBytes:   1 << 20,
			MaxWait:             50 * time.Millisecond,
			SessionTimeout:      6 * time.Second,
			HeartbeatInterval:   time.Second,
			RebalanceTimeout:    5 * time.Second,
			IsolationLevel:      "read_uncommitted",
			MaxPollRecords:      100,
			MessageQueueBatches: 16,
			CommitInterval:      200 * time.Millisecond,
			HandlerRetries:      0,
			HandlerRetryDelay:   20 * time.Millisecond,
		},
	}
}

// captureMetrics подменяет глобальный MeterProvider записывающим и возвращает
// журнал вызовов.
//
// Провайдер глобальный, поэтому тест, вызвавший captureMetrics, не должен быть
// параллельным: t.Parallel() в таком тесте перемешал бы записи с соседним.
func captureMetrics(t *testing.T) *recordedMetrics {
	t.Helper()

	rec := &recordedMetrics{}
	prev := otel.GetMeterProvider()

	otel.SetMeterProvider(recordingMeterProvider{rec: rec})
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	return rec
}

// mustProducer создаёт продюсер и закрывает его по завершении теста.
func mustProducer(t *testing.T, cfg Config) *Producer {
	t.Helper()

	p, err := NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	// Провал закрытия в Cleanup сам по себе тест не валит: сценарий уже
	// отработал, а часть тестов доводит продюсер до отказа намеренно. Но и
	// терять ошибку молча незачем — в журнале упавшего теста она объясняет,
	// почему брокер не отпустил соединение.
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Logf("Close в Cleanup: %v", err)
		}
	})

	return p
}

// mustConsumer создаёт консьюмер и останавливает его по завершении теста.
// Повторный Stop идемпотентен, поэтому Cleanup не мешает тесту остановить
// консьюмер самому.
func mustConsumer(t *testing.T, cfg Config) *Consumer {
	t.Helper()

	c, err := NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	t.Cleanup(func() {
		if err := c.Stop(); err != nil {
			t.Logf("Stop в Cleanup: %v", err)
		}
	})

	return c
}

// mustAddHandler регистрирует обработчик и валит тест при отказе: AddHandler
// в подготовительной части сценария — не то место, где ошибку можно потерять.
func mustAddHandler(t *testing.T, c *Consumer, topic string, h ConsumerHandler, mws ...ConsumerMiddleware) {
	t.Helper()

	if err := c.AddHandler(topic, h, mws...); err != nil {
		t.Fatalf("AddHandler(%q): %v", topic, err)
	}
}

// mockHandler — потокобезопасный тестовый обработчик сообщений.
//
// Поведение задаётся либо фиксированной ошибкой returnErr, либо функцией fn,
// которой доступен номер вызова (нумерация с 1) — так пишутся сценарии вида
// «первые две попытки падают, третья проходит».
type mockHandler struct {
	mu        sync.Mutex
	calls     int
	msgs      []IncomingMessage
	returnErr error
	fn        func(call int, msg IncomingMessage) error
}

func (h *mockHandler) ProcessMessage(_ context.Context, msg IncomingMessage) error {
	h.mu.Lock()
	h.calls++
	call := h.calls
	h.msgs = append(h.msgs, msg)
	fn, err := h.fn, h.returnErr
	h.mu.Unlock()

	if fn != nil {
		return fn(call, msg)
	}

	return err
}

func (h *mockHandler) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.calls
}

// messages возвращает снимок всех полученных сообщений в порядке вызовов.
func (h *mockHandler) messages() []IncomingMessage {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]IncomingMessage(nil), h.msgs...)
}

// waitFor опрашивает cond до истечения timeout и валит тест, если условие так
// и не наступило. Опрос, а не канал: условие обычно складывается из нескольких
// счётчиков, которые тест не контролирует поштучно.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("не дождались: %s (таймаут %s)", what, timeout)
}
