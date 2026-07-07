package kafkax

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestHeaders_Get(t *testing.T) {
	t.Parallel()

	headers := Headers{
		{Key: "content-type", Value: []byte("application/json")},
		{Key: "signature", Value: []byte{0x01, 0x02}},
	}

	if v, ok := headers.Get("signature"); !ok || string(v) != "\x01\x02" {
		t.Fatalf("Get(%q) = %v, %v, ожидалось найденное бинарное значение", "signature", v, ok)
	}
	if _, ok := headers.Get("missing"); ok {
		t.Fatal("Get() для отсутствующего ключа вернул ok=true")
	}
}

func TestHeaders_Get_DuplicateKeys_ReturnsFirst(t *testing.T) {
	t.Parallel()

	headers := Headers{
		{Key: "trace", Value: []byte("first")},
		{Key: "trace", Value: []byte("second")},
	}

	v, ok := headers.Get("trace")
	if !ok || string(v) != "first" {
		t.Fatalf("Get() = %v, %v, ожидалось первое значение дубликата", v, ok)
	}
}

func TestValidateHeaders_RejectsReservedKeys(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"traceparent", "tracestate", "baggage"} {
		headers := Headers{{Key: key, Value: []byte("x")}}
		if err := validateHeaders(headers); err == nil {
			t.Fatalf("validateHeaders() с зарезервированным ключом %q вернул nil, ожидалась ошибка", key)
		}
	}
}

func TestValidateHeaders_AllowsUserKeys(t *testing.T) {
	t.Parallel()

	headers := Headers{
		{Key: "signature", Value: []byte("x")},
		{Key: "content-type", Value: []byte("application/json")},
	}
	if err := validateHeaders(headers); err != nil {
		t.Fatalf("validateHeaders() с пользовательскими ключами вернул ошибку: %v", err)
	}
}

func TestToKafkaHeaders_RoundTrip(t *testing.T) {
	t.Parallel()

	original := Headers{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	}

	kafkaHeaders := toKafkaHeaders(original)
	if len(kafkaHeaders) != len(original) {
		t.Fatalf("toKafkaHeaders() вернул %d заголовков, ожидалось %d", len(kafkaHeaders), len(original))
	}

	roundTripped := fromKafkaHeaders(kafkaHeaders)
	for i, h := range roundTripped {
		if h.Key != original[i].Key || string(h.Value) != string(original[i].Value) {
			t.Fatalf("round-trip[%d] = %+v, ожидалось %+v", i, h, original[i])
		}
	}
}

func TestFromKafkaHeaders_NeverNil(t *testing.T) {
	t.Parallel()

	if headers := fromKafkaHeaders(nil); headers == nil {
		t.Fatal("fromKafkaHeaders(nil) вернул nil, ожидался пустой слайс")
	}
	if headers := fromKafkaHeaders([]kafka.Header{}); headers == nil {
		t.Fatal("fromKafkaHeaders([]) вернул nil, ожидался пустой слайс")
	}
}
