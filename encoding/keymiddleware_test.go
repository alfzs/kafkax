package encoding

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/alfzs/kafkax"
	"github.com/google/uuid"
)

func TestMatchKeyMiddleware_Match(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
	botID := "bot-1"

	key, err := EncodeKey(tenantID, botID)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	var calls atomic.Int64

	next := kafkax.ConsumerHandlerFunc(func(_ context.Context, _ kafkax.IncomingMessage) error {
		calls.Add(1)
		return nil
	})

	handler := MatchKeyMiddleware(tenantID, botID)(next)

	err = handler.ProcessMessage(t.Context(), kafkax.IncomingMessage{Key: key})
	if err != nil {
		t.Fatalf("ProcessMessage() вернул ошибку: %v", err)
	}

	if calls.Load() != 1 {
		t.Fatalf("next вызван %d раз, ожидалось 1", calls.Load())
	}
}

func TestMatchKeyMiddleware_NoMatch(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

	key, err := EncodeKey(tenantID, "bot-1")
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	var calls atomic.Int64

	next := kafkax.ConsumerHandlerFunc(func(_ context.Context, _ kafkax.IncomingMessage) error {
		calls.Add(1)
		return nil
	})

	// Тот же tenantID, но другой botID — валидный по длине ключ другого адресата.
	handler := MatchKeyMiddleware(tenantID, "bot-2")(next)

	err = handler.ProcessMessage(t.Context(), kafkax.IncomingMessage{Key: key})
	if err != nil {
		t.Fatalf("ProcessMessage() вернул ошибку: %v, ожидался тихий пропуск", err)
	}

	if calls.Load() != 0 {
		t.Fatalf("next вызван %d раз, ожидалось 0 (сообщение не для этого адресата)", calls.Load())
	}
}

func TestMatchKeyMiddleware_TooShortKey(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

	var calls atomic.Int64

	next := kafkax.ConsumerHandlerFunc(func(_ context.Context, _ kafkax.IncomingMessage) error {
		calls.Add(1)
		return nil
	})

	handler := MatchKeyMiddleware(tenantID, "bot-1")(next)

	// Ключ короче ожидаемого для tenantID+"bot-1" — усечённое/повреждённое сообщение.
	err := handler.ProcessMessage(t.Context(), kafkax.IncomingMessage{Key: []byte{1, 2, 3}})
	if err == nil {
		t.Fatal("ProcessMessage() вернул nil, ожидалась ошибка для усечённого ключа")
	}

	if !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("errors.Is(err, ErrInvalidKey) = false, err: %v", err)
	}

	if calls.Load() != 0 {
		t.Fatalf("next вызван %d раз, ожидалось 0 (повреждённый ключ не должен доходить до handler'а)", calls.Load())
	}
}

func TestMatchKeyMiddleware_NilKey(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

	handler := MatchKeyMiddleware(tenantID, "bot-1")(kafkax.ConsumerHandlerFunc(
		func(_ context.Context, _ kafkax.IncomingMessage) error { return nil }))

	err := handler.ProcessMessage(t.Context(), kafkax.IncomingMessage{Key: nil})
	if !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("errors.Is(err, ErrInvalidKey) = false, err: %v", err)
	}
}

func TestMatchKeyMiddleware_NextError(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
	botID := "bot-1"

	key, err := EncodeKey(tenantID, botID)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	wantErr := errors.New("handler failed")

	next := kafkax.ConsumerHandlerFunc(func(_ context.Context, _ kafkax.IncomingMessage) error {
		return wantErr
	})

	handler := MatchKeyMiddleware(tenantID, botID)(next)

	err = handler.ProcessMessage(t.Context(), kafkax.IncomingMessage{Key: key})
	if !errors.Is(err, wantErr) {
		t.Fatalf("ProcessMessage() = %v, ожидалась %v", err, wantErr)
	}
}
