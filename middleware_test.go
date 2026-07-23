package kafkax

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestConsumerHandlerFunc(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64

	handler := ConsumerHandlerFunc(func(_ context.Context, _ IncomingMessage) error {
		calls.Add(1)
		return nil
	})

	err := handler.ProcessMessage(t.Context(), IncomingMessage{})
	if err != nil {
		t.Fatalf("ProcessMessage() вернул неожиданную ошибку: %v", err)
	}

	if calls.Load() != 1 {
		t.Fatalf("ProcessMessage() вызван %d раз, ожидалось 1", calls.Load())
	}
}

func TestChain_NoMiddleware(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64

	handler := ConsumerHandlerFunc(func(_ context.Context, _ IncomingMessage) error {
		calls.Add(1)
		return nil
	})

	chained := Chain(handler)
	_ = chained.ProcessMessage(t.Context(), IncomingMessage{})

	if calls.Load() != 1 {
		t.Fatalf("ProcessMessage() вызван %d раз, ожидалось 1", calls.Load())
	}
}

func TestChain_SingleMiddleware(t *testing.T) {
	t.Parallel()

	var order []string

	handler := ConsumerHandlerFunc(func(_ context.Context, _ IncomingMessage) error {
		order = append(order, "handler")
		return nil
	})

	mw := func(next ConsumerHandler) ConsumerHandler {
		return ConsumerHandlerFunc(func(ctx context.Context, msg IncomingMessage) error {
			order = append(order, "mw-start")
			err := next.ProcessMessage(ctx, msg)

			order = append(order, "mw-end")

			return err
		})
	}

	chained := Chain(handler, mw)
	_ = chained.ProcessMessage(t.Context(), IncomingMessage{})

	expected := []string{"mw-start", "handler", "mw-end"}
	if len(order) != len(expected) || order[0] != expected[0] || order[1] != expected[1] || order[2] != expected[2] {
		t.Fatalf("порядок вызовов: %v, ожидалось %v", order, expected)
	}
}

func TestChain_MultipleMiddleware(t *testing.T) {
	t.Parallel()

	var order []string

	handler := ConsumerHandlerFunc(func(_ context.Context, _ IncomingMessage) error {
		order = append(order, "handler")
		return nil
	})

	mw1 := func(next ConsumerHandler) ConsumerHandler {
		return ConsumerHandlerFunc(func(ctx context.Context, msg IncomingMessage) error {
			order = append(order, "mw1-start")
			err := next.ProcessMessage(ctx, msg)

			order = append(order, "mw1-end")

			return err
		})
	}

	mw2 := func(next ConsumerHandler) ConsumerHandler {
		return ConsumerHandlerFunc(func(ctx context.Context, msg IncomingMessage) error {
			order = append(order, "mw2-start")
			err := next.ProcessMessage(ctx, msg)

			order = append(order, "mw2-end")

			return err
		})
	}

	// Chain(handler, mw1, mw2) → mw1(mw2(handler))
	// Порядок: mw1-start → mw2-start → handler → mw2-end → mw1-end
	chained := Chain(handler, mw1, mw2)
	_ = chained.ProcessMessage(t.Context(), IncomingMessage{})

	expected := []string{"mw1-start", "mw2-start", "handler", "mw2-end", "mw1-end"}
	if len(order) != len(expected) {
		t.Fatalf("порядок вызовов(%d): %v, ожидалось %v", len(order), order, expected)
	}

	for i := range expected {
		if order[i] != expected[i] {
			t.Fatalf("позиция %d: %q, ожидалось %q; полный порядок: %v", i, order[i], expected[i], order)
		}
	}
}

func TestChain_MiddlewareSkipsInner(t *testing.T) {
	t.Parallel()

	var innerCalls atomic.Int64

	handler := ConsumerHandlerFunc(func(_ context.Context, _ IncomingMessage) error {
		innerCalls.Add(1)
		return nil
	})

	filter := func(next ConsumerHandler) ConsumerHandler {
		return ConsumerHandlerFunc(func(ctx context.Context, msg IncomingMessage) error {
			// Фильтр: пропускаем сообщения с пустым Key
			if len(msg.Key) == 0 {
				return nil
			}

			return next.ProcessMessage(ctx, msg)
		})
	}

	chained := Chain(handler, filter)

	// Сообщение с пустым Key — фильтр должен скипнуть
	_ = chained.ProcessMessage(t.Context(), IncomingMessage{})

	if innerCalls.Load() != 0 {
		t.Fatalf("inner вызван %d раз, ожидалось 0 (фильтр должен скипнуть)", innerCalls.Load())
	}

	// Сообщение с непустым Key — фильтр пропускает
	_ = chained.ProcessMessage(t.Context(), IncomingMessage{Key: []byte("key")})

	if innerCalls.Load() != 1 {
		t.Fatalf("inner вызван %d раз, ожидалось 1 (фильтр должен пропустить)", innerCalls.Load())
	}
}

func TestChain_MiddlewareErrorPropagation(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("handler error")

	handler := ConsumerHandlerFunc(func(_ context.Context, _ IncomingMessage) error {
		return expectedErr
	})

	mw := func(next ConsumerHandler) ConsumerHandler {
		return ConsumerHandlerFunc(func(ctx context.Context, msg IncomingMessage) error {
			return next.ProcessMessage(ctx, msg)
		})
	}

	chained := Chain(handler, mw)
	err := chained.ProcessMessage(context.Background(), IncomingMessage{})

	if !errors.Is(err, expectedErr) {
		t.Fatalf("Chain() error = %v, ожидалось %v", err, expectedErr)
	}
}

// TestKafkaConsumer_AddHandler_Middleware проверяет, что middleware,
// переданные в AddHandler, корректно применяются к handler.
func TestKafkaConsumer_AddHandler_Middleware(t *testing.T) {
	t.Parallel()
	c := mustNewConsumerWithConfig(t, fastCommitConfig())

	var (
		outerCalls atomic.Int64
		innerCalls atomic.Int64
	)

	handler := ConsumerHandlerFunc(func(_ context.Context, _ IncomingMessage) error {
		innerCalls.Add(1)
		return nil
	})

	counter := func(next ConsumerHandler) ConsumerHandler {
		return ConsumerHandlerFunc(func(ctx context.Context, msg IncomingMessage) error {
			outerCalls.Add(1)
			return next.ProcessMessage(ctx, msg)
		})
	}

	if err := c.AddHandler("mw-test", handler, counter); err != nil {
		t.Fatalf("AddHandler() вернул неожиданную ошибку: %v", err)
	}

	// Вызываем handleMessage напрямую, как в TestKafkaConsumer_HandleMessage_RetriesAndSkipsAfterMaxRetries
	topic := "mw-test"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1},
		Value:          []byte("payload"),
	}

	done := make(chan struct{})

	go func() {
		c.handleMessage(t.Context(), msg)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleMessage() завис")
	}

	if outerCalls.Load() != 1 {
		t.Fatalf("middleware вызван %d раз, ожидалось 1", outerCalls.Load())
	}

	if innerCalls.Load() != 1 {
		t.Fatalf("handler вызван %d раз, ожидалось 1", innerCalls.Load())
	}
}

// TestKafkaConsumer_AddHandler_MiddlewareFilter проверяет, что middleware-
// фильтр (возвращающий nil без вызова next) предотвращает вызов handler.
func TestKafkaConsumer_AddHandler_MiddlewareFilter(t *testing.T) {
	t.Parallel()
	c := mustNewConsumerWithConfig(t, fastCommitConfig())

	var handlerCalls atomic.Int64

	handler := ConsumerHandlerFunc(func(_ context.Context, _ IncomingMessage) error {
		handlerCalls.Add(1)
		return nil
	})

	filter := func(next ConsumerHandler) ConsumerHandler {
		return ConsumerHandlerFunc(func(ctx context.Context, msg IncomingMessage) error {
			if len(msg.Key) == 0 {
				return nil
			}

			return next.ProcessMessage(ctx, msg)
		})
	}

	if err := c.AddHandler("mw-filter", handler, filter); err != nil {
		t.Fatalf("AddHandler() вернул неожиданную ошибку: %v", err)
	}

	topic := "mw-filter"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1},
		Value:          []byte("payload"),
		// Key не установлен — пустой, фильтр должен скипнуть
	}

	done := make(chan struct{})

	go func() {
		c.handleMessage(t.Context(), msg)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleMessage() завис")
	}

	if handlerCalls.Load() != 0 {
		t.Fatalf("handler вызван %d раз, ожидалось 0 (фильтр должен скипнуть)", handlerCalls.Load())
	}
}
