package kafkax

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

// kafkaHeaderCarrier реализует propagation.TextMapCarrier для передачи
// trace context через Kafka message headers (W3C TraceContext / Baggage).
type kafkaHeaderCarrier struct {
	headers *[]kafka.Header
}

func newKafkaHeaderCarrier(headers *[]kafka.Header) kafkaHeaderCarrier {
	return kafkaHeaderCarrier{headers: headers}
}

func (c kafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}

	return ""
}

func (c kafkaHeaderCarrier) Set(key, value string) {
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}

	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = h.Key
	}

	return keys
}
