package kafkax

import (
	"slices"
	"testing"

	"go.opentelemetry.io/otel"
)

// TestDurationHistogramsDeclareExplicitBuckets — обе гистограммы длительности
// регистрируются со своими границами, а не с умолчанием SDK.
//
// Проверка выглядит тавтологичной ровно до тех пор, пока опцию не потеряют при
// правке соседней строки. Цена потери непропорциональна: умолчание OTel SDK
// размечено под миллисекунды, инструменты объявлены в секундах, и с ним весь
// трафик быстрее пяти секунд оседает в первом бакете. Гистограмма при этом
// продолжает исправно считаться и экспортироваться — сломанной она выглядит
// только на графике квантилей, где p50 и p99 совпадают.
func TestDurationHistogramsDeclareExplicitBuckets(t *testing.T) { //nolint:paralleltest // подменяет глобальный MeterProvider
	rec := captureMetrics(t)

	// Инструменты регистрируются в конструкторах, и обоим достаточно
	// глобального MeterProvider: до брокера ни один из них не ходит.
	if _, err := newConsumerMetrics(otel.GetMeterProvider().Meter(instrumentationName)); err != nil {
		t.Fatalf("newConsumerMetrics: %v", err)
	}

	p := &KafkaProducer{}
	if err := p.initMetrics(); err != nil {
		t.Fatalf("initMetrics: %v", err)
	}

	cases := []struct {
		name string
		want []float64
	}{
		{"kafkax.consumer.message.duration", consumerDurationBuckets},
		{"kafkax.producer.message.duration", producerDurationBuckets},
	}

	for _, tc := range cases {
		if got := rec.bucketsOf(tc.name); !slices.Equal(got, tc.want) {
			t.Errorf("%s зарегистрирована с границами %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestDurationBucketsAreSaneGrids — сетки строго возрастают и начинаются с
// положительного значения.
//
// Требование не стилистическое: OTel SDK на неупорядоченных границах ведёт
// себя неопределённо, а нулевая или отрицательная нижняя граница создаёт бакет,
// в который не попадает ничего, — длительность неотрицательна по построению.
func TestDurationBucketsAreSaneGrids(t *testing.T) {
	t.Parallel()

	for name, buckets := range map[string][]float64{
		"consumerDurationBuckets": consumerDurationBuckets,
		"producerDurationBuckets": producerDurationBuckets,
	} {
		if len(buckets) == 0 {
			t.Errorf("%s пуст", name)

			continue
		}

		if buckets[0] <= 0 {
			t.Errorf("%s начинается с %v, want > 0", name, buckets[0])
		}

		for i := 1; i < len(buckets); i++ {
			if buckets[i] <= buckets[i-1] {
				t.Errorf("%s не возрастает: [%d]=%v, [%d]=%v",
					name, i-1, buckets[i-1], i, buckets[i])
			}
		}
	}
}
