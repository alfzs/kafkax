package kafkax

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
				Brokers:          []string{testInvalidBroker},
				ClientID:         testInvalidClientID,
				SecurityProtocol: SecurityProtocolSASLPlaintext,
				Consumer:         testConfig().Consumer,
			},
			errContains: envKeySASLUsername,
		},
		{
			name: "SASL_SSL только с username",
			config: Config{
				Brokers:          []string{testInvalidBroker},
				ClientID:         testInvalidClientID,
				SecurityProtocol: SecurityProtocolSASLSSL,
				SASL:             SASL{Username: testSASLUser},
				Consumer:         testConfig().Consumer,
			},
			errContains: envKeySASLPassword,
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
		_ = c.AddHandler(testTopic, &mockHandler{})

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

		err := c.Start(t.Context())
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

		if err := c.Start(t.Context()); err != nil {
			t.Skipf("пропуск: первый Start() завершился с ошибкой: %v", err)
		}

		t.Log("второй вызов Start() — должен вернуть ошибку")

		err := c.Start(t.Context())
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

// TestKafkaConsumer_HandleMessage_RetriesAndSkipsAfterMaxRetries проверяет
// retry/skip/commit-логику handleMessage напрямую, без брокера: *kafka.Message —
// обычная структура, поэтому handleMessage можно вызвать изнутри пакета,
// не проходя через runConsumerLoop/processMessage.
func TestKafkaConsumer_HandleMessage_RetriesAndSkipsAfterMaxRetries(t *testing.T) {
	t.Parallel()
	c := mustNewConsumerWithConfig(t, fastCommitConfig())

	handler := &mockHandler{returnErr: errors.New("boom")}

	topic := "retry-topic"
	if err := c.AddHandler(topic, handler); err != nil {
		t.Fatalf("AddHandler() вернул неожиданную ошибку: %v", err)
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 7},
		Value:          []byte("payload"),
	}

	done := make(chan struct{})

	go func() {
		c.handleMessage(t.Context(), msg)
		close(done)
	}()

	// CommitMessage без брокера блокируется на "Local: Waiting for coordinator"
	// вплоть до Consumer.SessionTimeout — fastCommitConfig() держит его коротким.
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleMessage() завис — вероятно, заблокирован на CommitMessage")
	}

	maxRetries := testConfig().Consumer.HandlerMaxRetries
	if calls := handler.callCount(); calls != maxRetries {
		t.Fatalf("ProcessMessage вызван %d раз(а), ожидалось ровно HandlerMaxRetries=%d", calls, maxRetries)
	}

	t.Logf("handleMessage() вызвал ProcessMessage ровно %d раз перед skip ✓", maxRetries)
}

// TestKafkaConsumer_HandleMessage_HeadersRoundTrip проверяет, что пользовательский
// заголовок, сконструированный так же, как в produce() (через toKafkaHeaders),
// доходит до IncomingMessage.Headers внутри handleMessage (через fromKafkaHeaders) —
// то есть проверяет границу produce()/handleMessage целиком, а не только
// toKafkaHeaders/fromKafkaHeaders по отдельности (см. headers_test.go).
func TestKafkaConsumer_HandleMessage_HeadersRoundTrip(t *testing.T) {
	t.Parallel()
	c := mustNewConsumerWithConfig(t, fastCommitConfig())

	handler := &mockHandler{}

	topic := "headers-roundtrip"
	if err := c.AddHandler(topic, handler); err != nil {
		t.Fatalf("AddHandler() вернул неожиданную ошибку: %v", err)
	}

	custom := Headers{{Key: "x-order-id", Value: []byte("order-123")}}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1},
		Value:          []byte("payload"),
		Headers:        toKafkaHeaders(custom),
	}

	done := make(chan struct{})

	go func() {
		c.handleMessage(t.Context(), msg)
		close(done)
	}()

	// CommitMessage без брокера блокируется на "Local: Waiting for coordinator"
	// вплоть до Consumer.SessionTimeout — fastCommitConfig() держит его коротким.
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleMessage() завис — вероятно, заблокирован на CommitMessage")
	}

	got, ok := handler.lastMessage().Headers.Get("x-order-id")
	if !ok || string(got) != "order-123" {
		t.Fatalf("IncomingMessage.Headers.Get(%q) = %q, %v, ожидалось %q, true", "x-order-id", got, ok, "order-123")
	}

	t.Log("пользовательский заголовок дошёл до handler через toKafkaHeaders → fromKafkaHeaders ✓")
}

// TestKafkaConsumer_StartContextCancel_StopsLoopsWithoutClose проверяет
// заявленное в докстринге Start (consumer.go:270-272) поведение: в отличие от
// KafkaProducer (см. producer.go:224-230), у консьюмера нет горутины-наблюдателя,
// доводящей отмену ctx до полноценного Stop(). Поэтому после отмены ctx
// consumer loop и cleanup loop останавливаются (drain), но consumer.Close() ещё
// не вызван — последующий явный Stop() должен по-прежнему штатно завершить
// работу в пределах GracefulTimeout, а не мгновенно вернуться как "уже остановлен".
func TestKafkaConsumer_StartContextCancel_StopsLoopsWithoutClose(t *testing.T) {
	t.Parallel()
	c := mustNewConsumer(t)

	handler := &mockHandler{}
	if err := c.AddHandler("ctx-cancel-topic", handler); err != nil {
		t.Fatalf("AddHandler() вернул неожиданную ошибку: %v", err)
	}

	if err := c.SubscribeAll(); err != nil {
		t.Fatalf("SubscribeAll() вернул неожиданную ошибку: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start() вернул неожиданную ошибку: %v", err)
	}

	t.Log("отменяем ctx, переданный в Start — в отличие от продюсера, это НЕ эквивалентно Stop()")
	cancel()

	done := make(chan struct{})

	go func() {
		c.Stop()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Stop() после отмены ctx завершился штатно в пределах GracefulTimeout ✓")
	case <-time.After(testConfig().GracefulTimeout + time.Second):
		t.Fatalf("Stop() после отмены ctx завис дольше GracefulTimeout=%s", testConfig().GracefulTimeout)
	}
}

// TestKafkaConsumer_ConcurrentStop проверяет отсутствие гонок при конкурентном
// вызове Stop() из нескольких горутин на одном инстансе — isStopping корректно
// реализован через atomic.Bool с CompareAndSwap, но раньше это проверялось
// только последовательными вызовами.
func TestKafkaConsumer_ConcurrentStop(t *testing.T) {
	t.Parallel()
	c := mustNewConsumer(t)

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			c.Stop()
		})
	}

	wg.Wait()

	t.Log("конкурентные Stop() завершились без гонок и паник ✓")
}
