package kafkax

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Header — заголовок Kafka-сообщения.
type Header struct {
	Key   string
	Value []byte
}

// Headers — упорядоченный список заголовков с поиском по ключу.
// Порядок и дубликаты ключей сохраняются, как в самом протоколе Kafka.
type Headers []Header

// Get возвращает значение первого заголовка с данным ключом.
func (h Headers) Get(key string) ([]byte, bool) {
	for _, kv := range h {
		if kv.Key == key {
			return kv.Value, true
		}
	}

	return nil, false
}

// Имена заголовков W3C Trace Context/Baggage, которыми управляет
// OTel-propagator (см. produce() в producer.go).
const (
	headerKeyTraceparent = "traceparent"
	headerKeyTracestate  = "tracestate"
	headerKeyBaggage     = "baggage"
)

// reservedHeaderKeys — имена заголовков, которыми управляет OTel-propagator.
// Пользовательские заголовки с этими именами запрещены, чтобы не терять
// данные при молчаливой перезаписи в kafkaHeaderCarrier.Set.
var reservedHeaderKeys = map[string]struct{}{
	headerKeyTraceparent: {},
	headerKeyTracestate:  {},
	headerKeyBaggage:     {},
}

// validateHeaders возвращает ошибку, если headers содержат зарезервированное
// имя, используемое для передачи trace context.
func validateHeaders(headers Headers) error {
	for _, h := range headers {
		if _, reserved := reservedHeaderKeys[h.Key]; reserved {
			return fmt.Errorf("header key %q is reserved for trace propagation", h.Key)
		}
	}

	return nil
}

// toKafkaHeaders конвертирует Headers в формат confluent-kafka-go на границе
// producer'а, непосредственно перед вызовом Produce.
func toKafkaHeaders(headers Headers) []kafka.Header {
	out := make([]kafka.Header, 0, len(headers))
	for _, h := range headers {
		out = append(out, kafka.Header{Key: h.Key, Value: h.Value})
	}

	return out
}

// fromKafkaHeaders конвертирует заголовки confluent-kafka-go в Headers на
// границе consumer'а, сразу после получения сообщения.
func fromKafkaHeaders(headers []kafka.Header) Headers {
	out := make(Headers, 0, len(headers))
	for _, h := range headers {
		out = append(out, Header{Key: h.Key, Value: h.Value})
	}

	return out
}
