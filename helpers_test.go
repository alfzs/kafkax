package kafkax

import (
	"context"
	"sync"
	"testing"
	"time"
)

// testConfig возвращает конфигурацию с короткими таймаутами для unit-тестов.
// Указывает несуществующий брокер — librdkafka подключается лениво, поэтому
// структурные и поведенческие тесты не требуют работающего Kafka.
func testConfig() Config {
	return Config{
		Brokers:          []string{"localhost:29092"},
		ClientID:         "kafkax-unit-test",
		SecurityProtocol: "PLAINTEXT",
		GracefulTimeout:  2 * time.Second,
		Producer: Producer{
			RequiredAcks:          1,
			AckTimeout:            300 * time.Millisecond,
			FlushTimeout:          500 * time.Millisecond,
			MaxRetries:            0,
			RetryBackoff:          100 * time.Millisecond,
			BatchSize:             10,
			BatchBytes:            1048576,
			BatchTimeout:          10 * time.Millisecond,
			CompressionType:       "none",
			MaxInflight:           5,
			EnableIdempotence:     false,
			MessageQueueSize:      16,
			MessageTimeout:        300 * time.Millisecond,
			InactiveWorkerTTL:     5 * time.Minute,
			CleanupWorkerInterval: 10 * time.Minute,
		},
		Consumer: Consumer{
			Group:                 "kafkax-unit-test-group",
			EnableAutoCommit:      false,
			InitialOffset:         "earliest",
			MinBytes:              1,
			MaxBytes:              1048576,
			MaxWait:               50 * time.Millisecond,
			SocketTimeout:         5 * time.Second,
			SessionTimeout:        10 * time.Second,
			HeartbeatInterval:     3 * time.Second,
			IsolationLevel:        "read_committed",
			MaxPollInterval:       30 * time.Second,
			ReadTimeout:           100 * time.Millisecond,
			ReadErrorBackoff:      50 * time.Millisecond,
			MessageQueueSize:      16,
			HandlerMaxRetries:     2,
			HandlerRetryDelay:     50 * time.Millisecond,
			InactiveWorkerTTL:     5 * time.Minute,
			CleanupWorkerInterval: 10 * time.Minute,
		},
	}
}

// mockHandler — потокобезопасный тестовый обработчик сообщений.
type mockHandler struct {
	mu        sync.Mutex
	calls     int
	returnErr error
	lastMsg   IncomingMessage
}

func (h *mockHandler) ProcessMessage(_ context.Context, msg IncomingMessage) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	h.lastMsg = msg
	return h.returnErr
}

func (h *mockHandler) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

func (h *mockHandler) lastMessage() IncomingMessage {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastMsg
}

// mustNewProducer создаёт продюсер для тестов.
// Пропускает тест, если librdkafka не может инициализировать клиент.
func mustNewProducer(t *testing.T) *KafkaProducer {
	t.Helper()
	p, err := NewKafkaProducer(context.Background(), testConfig())
	if err != nil {
		t.Skipf("пропуск: не удалось создать продюсер (librdkafka): %v", err)
	}
	t.Cleanup(p.Close)
	t.Logf("продюсер создан: client_id=%s broker=%s", testConfig().ClientID, testConfig().Brokers[0])
	return p
}

// fastCommitConfig возвращает testConfig() с уменьшенными SessionTimeout/
// SocketTimeout: без брокера синхронный CommitMessage блокируется на "Local:
// Waiting for coordinator" вплоть до SessionTimeout (10s в testConfig()) —
// тестам, вызывающим handleMessage напрямую, не нужна точная длительность
// таймаута группы, только его короткое и предсказуемое истечение.
func fastCommitConfig() Config {
	cfg := testConfig()
	cfg.Consumer.SessionTimeout = time.Second
	cfg.Consumer.SocketTimeout = time.Second
	cfg.Consumer.HeartbeatInterval = 300 * time.Millisecond
	return cfg
}

// mustNewConsumer создаёт консьюмер для тестов.
// Пропускает тест, если librdkafka не может инициализировать клиент.
func mustNewConsumer(t *testing.T) *KafkaConsumer {
	t.Helper()
	c, err := NewKafkaConsumer(testConfig())
	if err != nil {
		t.Skipf("пропуск: не удалось создать консьюмер (librdkafka): %v", err)
	}
	t.Cleanup(c.Stop)
	t.Logf("консьюмер создан: group=%s broker=%s", testConfig().Consumer.Group, testConfig().Brokers[0])
	return c
}

// mustNewConsumerWithConfig — вариант mustNewConsumer с явно переданным Config
// (например, fastCommitConfig() для тестов, вызывающих handleMessage напрямую).
func mustNewConsumerWithConfig(t *testing.T, cfg Config) *KafkaConsumer {
	t.Helper()
	c, err := NewKafkaConsumer(cfg)
	if err != nil {
		t.Skipf("пропуск: не удалось создать консьюмер (librdkafka): %v", err)
	}
	t.Cleanup(c.Stop)
	t.Logf("консьюмер создан: group=%s broker=%s", cfg.Consumer.Group, cfg.Brokers[0])
	return c
}
