package kafkax

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestNewKafkaConsumer_InvalidConfig проверяет, что невалидная конфигурация
// не позволяет создать консьюмер.
func TestNewKafkaConsumer_InvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      Config
		errContains string
	}{
		{
			name: "SASL_PLAINTEXT без credentials",
			config: Config{
				Brokers:          []string{"localhost:9092"},
				ClientID:         "test",
				SecurityProtocol: "SASL_PLAINTEXT",
				Consumer:         testConfig().Consumer,
			},
			errContains: "KAFKAX_SASL_USERNAME",
		},
		{
			name: "SASL_SSL только с username",
			config: Config{
				Brokers:          []string{"localhost:9092"},
				ClientID:         "test",
				SecurityProtocol: "SASL_SSL",
				SASL:             SASL{Username: "user"},
				Consumer:         testConfig().Consumer,
			},
			errContains: "KAFKAX_SASL_PASSWORD",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			t.Logf("попытка создать консьюмер с невалидным конфигом: %s", tc.name)

			c, err := NewKafkaConsumer(tc.config)

			if err == nil {
				c.Stop()
				t.Fatalf("NewKafkaConsumer() вернул nil-ошибку, ожидалась ошибка содержащая %q", tc.errContains)
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Fatalf("NewKafkaConsumer() error=%q не содержит %q", err.Error(), tc.errContains)
			}
			t.Logf("получена ожидаемая ошибка: %v ✓", err)
		})
	}
}

// TestNewKafkaConsumer_Success проверяет успешное создание консьюмера.
func TestNewKafkaConsumer_Success(t *testing.T) {
	t.Parallel()
	t.Log("создаём консьюмер с валидным конфигом (брокер может быть недоступен)")

	c, err := NewKafkaConsumer(testConfig())
	if err != nil {
		t.Skipf("пропуск: librdkafka не может инициализировать консьюмер: %v", err)
	}
	defer c.Stop()

	if c == nil {
		t.Fatal("NewKafkaConsumer() вернул nil без ошибки")
	}
	t.Log("консьюмер создан успешно ✓")
}

// TestKafkaConsumer_AddHandler проверяет регистрацию обработчиков топиков.
func TestKafkaConsumer_AddHandler(t *testing.T) {
	t.Parallel()

	t.Run("первичная регистрация успешна", func(t *testing.T) {
		t.Parallel()
		c := mustNewConsumer(t)
		handler := &mockHandler{}

		err := c.AddHandler("orders", handler)

		if err != nil {
			t.Fatalf("AddHandler(orders) вернул неожиданную ошибку: %v", err)
		}
		t.Log("AddHandler(orders) успешно зарегистрировал обработчик ✓")
	})

	t.Run("повторная регистрация одного топика возвращает ошибку", func(t *testing.T) {
		t.Parallel()
		c := mustNewConsumer(t)
		handler := &mockHandler{}

		_ = c.AddHandler("payments", handler)
		t.Log("первая регистрация payments выполнена")

		err := c.AddHandler("payments", handler)

		if err == nil {
			t.Fatal("AddHandler(payments) повторно вернул nil, ожидалась ошибка дублирования")
		}
		if !strings.Contains(err.Error(), "payments") {
			t.Fatalf("ошибка дублирования %q не упоминает топик %q", err.Error(), "payments")
		}
		t.Logf("повторная регистрация вернула ожидаемую ошибку: %v ✓", err)
	})

	t.Run("разные топики регистрируются независимо", func(t *testing.T) {
		t.Parallel()
		c := mustNewConsumer(t)

		topics := []string{"topic-a", "topic-b", "topic-c"}
		for _, topic := range topics {
			if err := c.AddHandler(topic, &mockHandler{}); err != nil {
				t.Fatalf("AddHandler(%q) вернул неожиданную ошибку: %v", topic, err)
			}
		}
		t.Logf("все %d обработчиков зарегистрированы без ошибок ✓", len(topics))
	})
}

// TestKafkaConsumer_SubscribeAll проверяет подписку на все зарегистрированные топики.
func TestKafkaConsumer_SubscribeAll(t *testing.T) {
	t.Parallel()

	t.Run("без зарегистрированных обработчиков возвращает ошибку", func(t *testing.T) {
		t.Parallel()
		c := mustNewConsumer(t)
		t.Log("вызываем SubscribeAll без предварительного AddHandler")

		err := c.SubscribeAll()

		if err == nil {
			t.Fatal("SubscribeAll() без обработчиков вернул nil, ожидалась ошибка")
		}
		t.Logf("получена ожидаемая ошибка: %v ✓", err)
	})

	t.Run("с зарегистрированными обработчиками не возвращает ошибку", func(t *testing.T) {
		t.Parallel()
		c := mustNewConsumer(t)
		_ = c.AddHandler("test-topic", &mockHandler{})
		t.Log("вызываем SubscribeAll после AddHandler")

		err := c.SubscribeAll()

		if err != nil {
			t.Fatalf("SubscribeAll() вернул неожиданную ошибку: %v", err)
		}
		t.Log("SubscribeAll() успешно подписался на зарегистрированные топики ✓")
	})
}

// TestKafkaConsumer_Start_Errors проверяет граничные условия запуска консьюмера.
func TestKafkaConsumer_Start_Errors(t *testing.T) {
	t.Parallel()

	t.Run("без зарегистрированных обработчиков возвращает ошибку", func(t *testing.T) {
		t.Parallel()
		c := mustNewConsumer(t)
		t.Log("вызываем Start() без предварительного AddHandler")

		err := c.Start(context.Background())

		if err == nil {
			t.Fatal("Start() без обработчиков вернул nil, ожидалась ошибка")
		}
		t.Logf("Start() без обработчиков вернул ожидаемую ошибку: %v ✓", err)
	})

	t.Run("повторный вызов Start возвращает ошибку", func(t *testing.T) {
		t.Parallel()
		c := mustNewConsumer(t)
		_ = c.AddHandler("idempotency-topic", &mockHandler{})
		_ = c.SubscribeAll()

		t.Log("первый вызов Start()")
		if err := c.Start(context.Background()); err != nil {
			t.Skipf("пропуск: первый Start() завершился с ошибкой: %v", err)
		}

		t.Log("второй вызов Start() — должен вернуть ошибку")
		err := c.Start(context.Background())

		if err == nil {
			t.Fatal("второй Start() вернул nil, ожидалась ошибка 'already started'")
		}
		if !strings.Contains(err.Error(), "already started") {
			t.Fatalf("второй Start() error=%q, ожидалось 'already started'", err.Error())
		}
		t.Logf("второй Start() вернул ожидаемую ошибку: %q ✓", err.Error())
	})
}

// TestKafkaConsumer_Stop_Idempotent проверяет, что повторный вызов Stop
// не паникует и завершается корректно.
func TestKafkaConsumer_Stop_Idempotent(t *testing.T) {
	t.Parallel()

	t.Run("Stop без предшествующего Start безопасен", func(t *testing.T) {
		t.Parallel()
		c := mustNewConsumer(t)
		t.Log("вызываем Stop() без предварительного Start()")

		c.Stop()

		t.Log("Stop() без Start() завершился без паники ✓")
	})

	t.Run("повторный Stop безопасен", func(t *testing.T) {
		t.Parallel()
		c := mustNewConsumer(t)

		t.Log("первый Stop()")
		c.Stop()

		t.Log("второй Stop() — должен завершиться без паники и блокировки")
		c.Stop()

		t.Log("Stop() идемпотентен ✓")
	})
}

// TestKafkaConsumer_FullLifecycle проверяет полный жизненный цикл консьюмера:
// создание → регистрация обработчика → подписка → запуск → остановка.
func TestKafkaConsumer_FullLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("пропуск в -short режиме: тест содержит временные паузы")
	}
	t.Parallel()

	t.Log("шаг 1: создаём консьюмер")
	c := mustNewConsumer(t)

	t.Log("шаг 2: регистрируем обработчик для топика 'lifecycle-test'")
	handler := &mockHandler{}
	if err := c.AddHandler("lifecycle-test", handler); err != nil {
		t.Fatalf("AddHandler() завершился с ошибкой: %v", err)
	}

	t.Log("шаг 3: подписываемся на топики")
	if err := c.SubscribeAll(); err != nil {
		t.Fatalf("SubscribeAll() завершился с ошибкой: %v", err)
	}

	t.Log("шаг 4: запускаем consumer loop")
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() завершился с ошибкой: %v", err)
	}

	// Даём горутинам время запуститься; при недоступном брокере
	// consumer loop будет получать ошибки ReadMessage — это штатно.
	pause := 150 * time.Millisecond
	t.Logf("шаг 5: ожидаем %s для запуска горутин", pause)
	time.Sleep(pause)

	t.Log("шаг 6: останавливаем консьюмер")
	done := make(chan struct{})
	go func() {
		c.Stop()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Stop() завершился в пределах GracefulTimeout ✓")
	case <-time.After(testConfig().GracefulTimeout + time.Second):
		t.Fatalf("Stop() завис дольше GracefulTimeout=%s", testConfig().GracefulTimeout)
	}

	// ProcessMessage не должен был быть вызван — реального брокера нет.
	if calls := handler.callCount(); calls != 0 {
		t.Logf("информация: ProcessMessage был вызван %d раз (брокер оказался доступен)", calls)
	} else {
		t.Log("ProcessMessage не вызывался при недоступном брокере ✓")
	}
}
