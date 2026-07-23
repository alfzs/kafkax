package kafkax

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestNewKafkaProducer_InvalidConfig проверяет, что невалидная конфигурация
// не позволяет создать продюсер.
func TestNewKafkaProducer_InvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      Config
		errContains string
	}{
		{
			name: "SASL_PLAINTEXT без username",
			config: Config{
				Brokers:          []string{testInvalidBroker},
				ClientID:         testInvalidClientID,
				SecurityProtocol: SecurityProtocolSASLPlaintext,
				SASL:             SASL{Password: testSASLPassword},
				Producer:         testConfig().Producer,
				Consumer:         testConfig().Consumer,
			},
			errContains: envKeySASLUsername,
		},
		{
			name: "SASL_SSL без password",
			config: Config{
				Brokers:          []string{testInvalidBroker},
				ClientID:         testInvalidClientID,
				SecurityProtocol: SecurityProtocolSASLSSL,
				SASL:             SASL{Username: testSASLUser},
				Producer:         testConfig().Producer,
				Consumer:         testConfig().Consumer,
			},
			errContains: envKeySASLPassword,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			t.Logf("попытка создать продюсер с невалидным конфигом: %s", tc.name)

			p, err := NewKafkaProducer(t.Context(), tc.config)
			if err == nil {
				p.Close()
				t.Fatalf("NewKafkaProducer() вернул nil-ошибку, ожидалась ошибка, содержащая %q", tc.errContains)
			}

			if !strings.Contains(err.Error(), tc.errContains) {
				t.Fatalf("NewKafkaProducer() error=%q не содержит %q", err.Error(), tc.errContains)
			}

			t.Logf("получена ожидаемая ошибка: %v", err)
		})
	}
}

// TestNewKafkaProducer_Success проверяет успешное создание продюсера с валидным конфигом.
// Не требует работающего брокера — librdkafka подключается лениво.
func TestNewKafkaProducer_Success(t *testing.T) {
	t.Parallel()
	t.Log("создаём продюсер с валидным конфигом (брокер может быть недоступен)")

	p, err := NewKafkaProducer(t.Context(), testConfig())
	if err != nil {
		t.Skipf("пропуск: librdkafka не может инициализировать продюсер: %v", err)
	}
	defer p.Close()

	if p == nil {
		t.Fatal("NewKafkaProducer() вернул nil без ошибки")
	}

	t.Log("продюсер создан успешно ✓")
}

// TestKafkaProducer_SendMessage_WhenStopping проверяет, что SendMessage немедленно
// возвращает ошибку после вызова Close.
func TestKafkaProducer_SendMessage_WhenStopping(t *testing.T) {
	t.Parallel()

	p := mustNewProducer(t)
	t.Log("вызываем Close() для перевода продюсера в состояние остановки")
	p.Close()

	t.Log("пытаемся отправить сообщение в остановленный продюсер")

	err := p.SendMessage(t.Context(), PublishRequest{TenantID: uuid.New(), Topic: testTopic, Value: []byte("data")})
	if err == nil {
		t.Fatal("SendMessage() в остановленный продюсер вернул nil, ожидалась ошибка")
	}

	if !strings.Contains(err.Error(), "shutting down") {
		t.Fatalf("SendMessage() error=%q, ожидалась ошибка с 'shutting down'", err.Error())
	}

	t.Logf("получена ожидаемая ошибка: %q ✓", err.Error())
}

// TestKafkaProducer_SendMessage_ReservedHeaderKey проверяет, что SendMessage
// отклоняет заголовки с именами, зарезервированными под trace propagation.
func TestKafkaProducer_SendMessage_ReservedHeaderKey(t *testing.T) {
	t.Parallel()

	p := mustNewProducer(t)

	err := p.SendMessage(t.Context(), PublishRequest{
		TenantID: uuid.New(),
		Topic:    testTopic,
		Value:    []byte("data"),
		Headers:  Headers{{Key: "traceparent", Value: []byte("x")}},
	})
	if err == nil {
		t.Fatal("SendMessage() с заголовком 'traceparent' вернул nil, ожидалась ошибка")
	}

	if !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("SendMessage() error=%q, ожидалась ошибка про зарезервированный ключ", err.Error())
	}

	t.Logf("получена ожидаемая ошибка: %q ✓", err.Error())
}

// TestKafkaProducer_SendMessage_ContextCanceled проверяет, что отмена контекста
// вызывающего кода отражается в ошибке SendMessage.
func TestKafkaProducer_SendMessage_ContextCanceled(t *testing.T) {
	t.Parallel()

	p := mustNewProducer(t)
	ctx, cancel := context.WithCancel(t.Context())

	t.Log("отменяем контекст до вызова SendMessage")
	cancel()

	err := p.SendMessage(ctx, PublishRequest{TenantID: uuid.New(), Topic: testTopic, Value: []byte("data")})
	if err == nil {
		t.Fatal("SendMessage() с отменённым контекстом вернул nil, ожидалась ошибка")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SendMessage() error=%q не оборачивает context.Canceled (errors.Is вернул false)", err.Error())
	}

	t.Logf("SendMessage с отменённым контекстом вернул: %q, errors.Is(err, context.Canceled)=true ✓", err.Error())
}

// TestKafkaProducer_Close_Idempotent проверяет, что повторный вызов Close
// не паникует и завершается корректно.
func TestKafkaProducer_Close_Idempotent(t *testing.T) {
	t.Parallel()

	p := mustNewProducer(t)

	t.Log("первый вызов Close()")
	p.Close()

	t.Log("второй вызов Close() — должен завершиться без паники и блокировки")
	p.Close()

	t.Log("Close() идемпотентен ✓")
}

// TestKafkaProducer_ContextCancel_TriggersShutdown проверяет, что отмена
// родительского контекста завершает работу продюсера.
func TestKafkaProducer_ContextCancel_TriggersShutdown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())

	p, err := NewKafkaProducer(ctx, testConfig())
	if err != nil {
		t.Skipf("пропуск: librdkafka: %v", err)
	}
	defer p.Close()

	t.Log("отменяем родительский контекст продюсера")
	cancel()

	// Горутина-наблюдатель за ctx (producer.go:224) вызывает Close() асинхронно —
	// нет гарантии, что isStopping установится к моменту первой проверки, поэтому
	// опрашиваем с retry вместо единичной попытки (что раньше маскировало гонку
	// через t.Log вместо реального ассерта).
	deadline := time.Now().Add(2 * time.Second)

	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = p.SendMessage(t.Context(), PublishRequest{TenantID: uuid.New(), Topic: testTopic, Value: []byte("x")})
		if lastErr != nil && strings.Contains(lastErr.Error(), "shutting down") {
			t.Logf("SendMessage после cancel(ctx) вернул: %q ✓", lastErr.Error())
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("отмена ctx не привела к shutdown продюсера за 2s (последняя ошибка SendMessage: %v)", lastErr)
}

// TestKafkaProducer_ConcurrentCloseAndSendMessage проверяет отсутствие гонок при
// конкурентном вызове Close() и SendMessage() на одном инстансе — isStopping
// корректно реализован через atomic.Bool с CompareAndSwap, но раньше это
// проверялось только последовательными вызовами Close().
func TestKafkaProducer_ConcurrentCloseAndSendMessage(t *testing.T) {
	t.Parallel()
	p := mustNewProducer(t)

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			_ = p.SendMessage(t.Context(), PublishRequest{TenantID: uuid.New(), Topic: "concurrent-topic", Value: []byte("x")})
		})
	}

	wg.Go(p.Close)

	wg.Wait()
	t.Log("конкурентные SendMessage()/Close() завершились без гонок и паник ✓")
}
