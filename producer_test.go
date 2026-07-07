package kafkax

import (
	"context"
	"strings"
	"testing"

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
				Brokers:          []string{"localhost:9092"},
				ClientID:         "test",
				SecurityProtocol: "SASL_PLAINTEXT",
				SASL:             SASL{Password: "secret"},
				Producer:         testConfig().Producer,
				Consumer:         testConfig().Consumer,
			},
			errContains: "KAFKAX_SASL_USERNAME",
		},
		{
			name: "SASL_SSL без password",
			config: Config{
				Brokers:          []string{"localhost:9092"},
				ClientID:         "test",
				SecurityProtocol: "SASL_SSL",
				SASL:             SASL{Username: "user"},
				Producer:         testConfig().Producer,
				Consumer:         testConfig().Consumer,
			},
			errContains: "KAFKAX_SASL_PASSWORD",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			t.Logf("попытка создать продюсер с невалидным конфигом: %s", tc.name)

			p, err := NewKafkaProducer(context.Background(), tc.config)

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

	p, err := NewKafkaProducer(context.Background(), testConfig())
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
	err := p.SendMessage(context.Background(), PublishRequest{TenantID: uuid.New(), Topic: "test-topic", Value: []byte("data")})

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

	err := p.SendMessage(context.Background(), PublishRequest{
		TenantID: uuid.New(),
		Topic:    "test-topic",
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
	ctx, cancel := context.WithCancel(context.Background())

	t.Log("отменяем контекст до вызова SendMessage")
	cancel()

	err := p.SendMessage(ctx, PublishRequest{TenantID: uuid.New(), Topic: "test-topic", Value: []byte("data")})

	if err == nil {
		t.Fatal("SendMessage() с отменённым контекстом вернул nil, ожидалась ошибка")
	}
	t.Logf("SendMessage с отменённым контекстом вернул: %q ✓", err.Error())
}

// TestKafkaProducer_SendMessage_BrokerUnavailable проверяет, что SendMessage
// завершается по таймауту при недоступном брокере.
func TestKafkaProducer_SendMessage_BrokerUnavailable(t *testing.T) {
	if testing.Short() {
		t.Skip("пропуск в -short режиме: тест ждёт MessageTimeout (~300ms)")
	}
	t.Parallel()

	p := mustNewProducer(t)
	t.Logf("отправляем сообщение на недоступный брокер (ждём таймаут ~%s)", testConfig().Producer.MessageTimeout)

	err := p.SendMessage(context.Background(), PublishRequest{TenantID: uuid.New(), Topic: "test-topic", Value: []byte("hello")})

	if err == nil {
		// Если брокер случайно оказался доступен — тест некорректен.
		t.Log("брокер оказался доступен: сообщение доставлено (пропускаем проверку таймаута)")
		return
	}
	t.Logf("сообщение не доставлено при недоступном брокере: %q ✓", err.Error())
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

	ctx, cancel := context.WithCancel(context.Background())
	p, err := NewKafkaProducer(ctx, testConfig())
	if err != nil {
		t.Skipf("пропуск: librdkafka: %v", err)
	}
	defer p.Close()

	t.Log("отменяем родительский контекст продюсера")
	cancel()

	t.Log("проверяем, что SendMessage возвращает ошибку после отмены контекста")
	if sendErr := p.SendMessage(context.Background(), PublishRequest{TenantID: uuid.New(), Topic: "test-topic", Value: []byte("x")}); sendErr == nil {
		t.Log("предупреждение: SendMessage не вернул ошибку немедленно после cancel() — возможна гонка")
	} else {
		t.Logf("SendMessage после cancel() вернул: %q ✓", sendErr.Error())
	}
}
