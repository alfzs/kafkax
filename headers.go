package kafkax

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
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
// OTel-propagator внутри kotel.
const (
	headerKeyTraceparent = "traceparent"
	headerKeyTracestate  = "tracestate"
	headerKeyBaggage     = "baggage"
)

// reservedHeaderKeys — имена заголовков, которыми управляет OTel-propagator.
// Пользовательские заголовки с этими именами запрещены, чтобы не терять
// данные при молчаливой перезаписи в kotel.RecordCarrier.Set.
var reservedHeaderKeys = map[string]struct{}{
	headerKeyTraceparent: {},
	headerKeyTracestate:  {},
	headerKeyBaggage:     {},
}

// validateHeaders возвращает ошибку, если headers содержат пустое или
// зарезервированное имя (последние используются для передачи trace context).
func validateHeaders(headers Headers) error {
	for i, h := range headers {
		if h.Key == "" {
			return fmt.Errorf("header %d: %w", i, ErrEmptyHeaderKey)
		}

		if _, reserved := reservedHeaderKeys[h.Key]; reserved {
			return fmt.Errorf("header %d (%q): %w", i, h.Key, ErrReservedHeaderKey)
		}
	}

	return nil
}

// toRecordHeaders конвертирует Headers в формат franz-go на границе продюсера,
// непосредственно перед вызовом Produce.
func toRecordHeaders(headers Headers) []kgo.RecordHeader {
	if len(headers) == 0 {
		return nil
	}

	out := make([]kgo.RecordHeader, 0, len(headers))
	for _, h := range headers {
		out = append(out, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}

	return out
}

// fromRecordHeaders конвертирует заголовки franz-go в Headers на границе
// консьюмера, сразу после получения записи.
func fromRecordHeaders(headers []kgo.RecordHeader) Headers {
	if len(headers) == 0 {
		return nil
	}

	out := make(Headers, 0, len(headers))
	for _, h := range headers {
		out = append(out, Header{Key: h.Key, Value: h.Value})
	}

	return out
}
