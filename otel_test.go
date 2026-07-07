package kafkax

import (
	"context"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TestKafkaHeaderCarrier_Get проверяет чтение значений из заголовков Kafka.
func TestKafkaHeaderCarrier_Get(t *testing.T) {
	t.Parallel()

	headers := []kafka.Header{
		{Key: headerKeyTraceparent, Value: []byte("00-aabbccdd-eeff0011-01")},
		{Key: "x-custom", Value: []byte("custom-value")},
	}
	carrier := newKafkaHeaderCarrier(&headers)

	t.Run("возвращает значение существующего ключа", func(t *testing.T) {
		t.Parallel()

		got := carrier.Get(headerKeyTraceparent)
		if got != "00-aabbccdd-eeff0011-01" {
			t.Fatalf("Get(%q)=%q, ожидалось %q", headerKeyTraceparent, got, "00-aabbccdd-eeff0011-01")
		}

		t.Logf("Get(traceparent)=%q ✓", got)
	})

	t.Run("возвращает пустую строку для отсутствующего ключа", func(t *testing.T) {
		t.Parallel()

		got := carrier.Get("non-existent-key")
		if got != "" {
			t.Fatalf("Get(отсутствующий)=%q, ожидалась пустая строка", got)
		}

		t.Log(`Get(non-existent-key)="" ✓`)
	})

	t.Run("поиск чувствителен к регистру", func(t *testing.T) {
		t.Parallel()

		got := carrier.Get("Traceparent")
		if got != "" {
			t.Fatalf("Get(%q)=%q — Kafka headers чувствительны к регистру, ожидалась пустая строка", "Traceparent", got)
		}

		t.Log("Kafka headers чувствительны к регистру: Get(Traceparent)=\"\" ✓")
	})
}

// TestKafkaHeaderCarrier_Set проверяет добавление и обновление заголовков.
func TestKafkaHeaderCarrier_Set(t *testing.T) {
	t.Parallel()

	t.Run("добавляет новый ключ в пустой срез", func(t *testing.T) {
		t.Parallel()

		headers := make([]kafka.Header, 0)
		carrier := newKafkaHeaderCarrier(&headers)

		carrier.Set(headerKeyTraceparent, "00-trace-span-01")

		if len(headers) != 1 {
			t.Fatalf("после Set: len(headers)=%d, ожидалось 1", len(headers))
		}

		if headers[0].Key != headerKeyTraceparent || string(headers[0].Value) != "00-trace-span-01" {
			t.Fatalf("после Set: header={%q,%q}, ожидалось {%q,%q}",
				headers[0].Key, headers[0].Value, headerKeyTraceparent, "00-trace-span-01")
		}

		t.Logf("Set добавил заголовок: {%q: %q}", headers[0].Key, string(headers[0].Value))
	})

	t.Run("обновляет существующий ключ без дублирования", func(t *testing.T) {
		t.Parallel()

		headers := []kafka.Header{{Key: headerKeyTraceparent, Value: []byte("old-value")}}
		carrier := newKafkaHeaderCarrier(&headers)

		carrier.Set(headerKeyTraceparent, "new-value")

		if len(headers) != 1 {
			t.Fatalf("после обновления: len(headers)=%d, ожидалось 1 (дубликат не должен добавляться)", len(headers))
		}

		if string(headers[0].Value) != "new-value" {
			t.Fatalf("после обновления: value=%q, ожидалось %q", string(headers[0].Value), "new-value")
		}

		t.Logf("Set обновил существующий заголовок на месте: %q → %q", "old-value", "new-value")
	})

	t.Run("не затрагивает другие ключи", func(t *testing.T) {
		t.Parallel()

		headers := []kafka.Header{
			{Key: "other", Value: []byte("unchanged")},
			{Key: headerKeyTraceparent, Value: []byte("old")},
		}
		carrier := newKafkaHeaderCarrier(&headers)

		carrier.Set(headerKeyTraceparent, "new")

		if string(headers[0].Value) != "unchanged" {
			t.Fatalf("Set изменил несвязанный заголовок: %q", string(headers[0].Value))
		}

		t.Log("Set не затронул другие заголовки ✓")
	})
}

// TestKafkaHeaderCarrier_Keys проверяет получение списка ключей.
func TestKafkaHeaderCarrier_Keys(t *testing.T) {
	t.Parallel()

	t.Run("возвращает все ключи заголовков", func(t *testing.T) {
		t.Parallel()

		headers := []kafka.Header{
			{Key: headerKeyTraceparent, Value: []byte("v1")},
			{Key: headerKeyTracestate, Value: []byte("v2")},
			{Key: headerKeyBaggage, Value: []byte("v3")},
		}
		carrier := newKafkaHeaderCarrier(&headers)

		keys := carrier.Keys()

		if len(keys) != 3 {
			t.Fatalf("Keys() вернул %d ключей, ожидалось 3: %v", len(keys), keys)
		}

		expected := map[string]bool{headerKeyTraceparent: true, headerKeyTracestate: true, headerKeyBaggage: true}
		for _, k := range keys {
			if !expected[k] {
				t.Fatalf("Keys() вернул неожиданный ключ %q", k)
			}
		}

		t.Logf("Keys()=%v ✓", keys)
	})

	t.Run("возвращает пустой срез для пустого носителя", func(t *testing.T) {
		t.Parallel()

		carrier := newKafkaHeaderCarrier(new(make([]kafka.Header, 0)))

		keys := carrier.Keys()

		if len(keys) != 0 {
			t.Fatalf("Keys() для пустых headers=%v, ожидался []", keys)
		}

		t.Log("Keys() для пустого носителя → [] ✓")
	})
}

// TestKafkaHeaderCarrier_PropagationRoundTrip проверяет полный цикл inject→extract
// трейс-контекста через заголовки Kafka с использованием W3C TraceContext propagator.
func TestKafkaHeaderCarrier_PropagationRoundTrip(t *testing.T) {
	t.Parallel()
	t.Log("проверяем цикл: inject W3C TraceContext → Kafka headers → extract")

	prop := propagation.TraceContext{}

	traceID := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID := trace.SpanID{0xA, 0xB, 0xC, 0xD, 0xE, 0xF, 0x1, 0x2}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})

	// Inject: вставляем trace context в заголовки Kafka.
	injectCtx := trace.ContextWithSpanContext(context.Background(), sc)
	headers := make([]kafka.Header, 0, 2)
	carrier := newKafkaHeaderCarrier(&headers)
	prop.Inject(injectCtx, carrier)

	t.Logf("после Inject: заголовки=%v", headers)

	// Проверяем, что traceparent был записан в заголовки.
	traceparent := carrier.Get(headerKeyTraceparent)
	if traceparent == "" {
		t.Fatal("propagator не записал заголовок traceparent в Kafka headers")
	}

	t.Logf("traceparent=%s", traceparent)

	// W3C traceparent: "00-<traceID>-<spanID>-<flags>"
	// Проверяем hex-кодированные идентификаторы в значении заголовка.
	if !strings.Contains(traceparent, strings.ToLower(traceID.String())) {
		t.Fatalf("traceparent=%q не содержит trace_id=%s", traceparent, traceID)
	}

	if !strings.Contains(traceparent, strings.ToLower(spanID.String())) {
		t.Fatalf("traceparent=%q не содержит span_id=%s", traceparent, spanID)
	}

	t.Logf("trace_id и span_id корректно закодированы в traceparent ✓")

	// Extract: восстанавливаем trace context из заголовков.
	extractCtx := prop.Extract(context.Background(), carrier)
	extractedSC := trace.SpanContextFromContext(extractCtx)

	if !extractedSC.IsValid() {
		t.Fatal("извлечённый SpanContext невалиден после round-trip")
	}

	if extractedSC.TraceID() != traceID {
		t.Fatalf("TraceID не совпадает: got=%s, want=%s", extractedSC.TraceID(), traceID)
	}

	if extractedSC.SpanID() != spanID {
		t.Fatalf("SpanID не совпадает: got=%s, want=%s", extractedSC.SpanID(), spanID)
	}

	if !extractedSC.IsSampled() {
		t.Fatal("флаг Sampled не сохранился при round-trip")
	}

	t.Logf("round-trip: trace_id=%s span_id=%s sampled=%v ✓",
		extractedSC.TraceID(), extractedSC.SpanID(), extractedSC.IsSampled())
}
