package kafkax

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

// Бенчмарки стоимости инструментации на сообщение.
//
// Меряется ровно обвязка: ни брокера, ни сети, ни обработчика здесь нет. Это
// сознательно — e2e против kfake на четырёх ядрах шумит на десятки процентов и
// прячет разницу в сотни наносекунд, ради которой эти правки и делались.
//
// Каждый инструмент прогоняется на двух метрах:
//
//   - noop — видна только цена вызывающей стороны: сборка вариадика,
//     боксирование опции, построение attribute.Set;
//   - resolving — плюс цена стороны SDK: metric.NewAddConfig(opts) разбирает
//     опции в attribute.Set ровно так же, как это делает настоящий экспортёр.
//     Реальный SDK в зависимости пакета не тянется (см. recorders_test.go),
//     поэтому разбор опций воспроизводится тем же публичным API.
//
// Разделение важно: metric.WithAttributeSet экономит и там, и там, но по
// разным причинам, и объединённое число прятало бы половину эффекта.

// benchSink не даёт компилятору выбросить результат разбора опций.
var benchSink attribute.Set

// resolvingMeter — метр, чьи инструменты разбирают переданные опции в
// attribute.Set и выбрасывают результат.
//
// Отличается от recordingMeter из recorders_test.go отсутствием журнала:
// мьютекс и растущий слайс добавили бы к измерению аллокации, к
// инструментации отношения не имеющие.
type resolvingMeter struct {
	metricnoop.Meter
}

func (m resolvingMeter) Int64Counter(_ string, _ ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return resolvingCounter{}, nil
}

func (m resolvingMeter) Int64UpDownCounter(
	_ string, _ ...metric.Int64UpDownCounterOption,
) (metric.Int64UpDownCounter, error) {
	return resolvingUpDownCounter{}, nil
}

func (m resolvingMeter) Float64Histogram(
	_ string, _ ...metric.Float64HistogramOption,
) (metric.Float64Histogram, error) {
	return resolvingHistogram{}, nil
}

type resolvingCounter struct {
	metricnoop.Int64Counter
}

func (resolvingCounter) Add(_ context.Context, _ int64, opts ...metric.AddOption) {
	benchSink = metric.NewAddConfig(opts).Attributes()
}

type resolvingUpDownCounter struct {
	metricnoop.Int64UpDownCounter
}

func (resolvingUpDownCounter) Add(_ context.Context, _ int64, opts ...metric.AddOption) {
	benchSink = metric.NewAddConfig(opts).Attributes()
}

type resolvingHistogram struct {
	metricnoop.Float64Histogram
}

func (resolvingHistogram) Record(_ context.Context, _ float64, opts ...metric.RecordOption) {
	benchSink = metric.NewRecordConfig(opts).Attributes()
}

// benchMeters — оба метра под именами, которые уедут в имя бенчмарка.
func benchMeters() map[string]metric.Meter {
	return map[string]metric.Meter{
		"noop":      metricnoop.Meter{},
		"resolving": resolvingMeter{},
	}
}

// benchProducer собирает продюсер без клиента: recordSend трогает только
// инструменты, поэтому всё остальное намеренно нулевое.
func benchProducer(b *testing.B, meter metric.Meter) *KafkaProducer {
	b.Helper()

	p := &KafkaProducer{}
	if err := p.initMetrics(meter); err != nil {
		b.Fatalf("initMetrics: %v", err)
	}

	return p
}

// benchConsumer собирает консьюмер без клиента и с зарегистрированным
// обработчиком: набор топиков консьюмера — это набор его обработчиков.
func benchConsumer(b *testing.B, meter metric.Meter) *KafkaConsumer {
	b.Helper()

	// Поля повторяют NewKafkaConsumer в той части, которой касается путь
	// сообщения: клиент, конфигурация и жизненный контекст здесь не нужны.
	c := &KafkaConsumer{
		handlers: make(map[string]ConsumerHandler),
		opts:     newOptsCache(0),
	}

	m, err := newConsumerMetrics(meter)
	if err != nil {
		b.Fatalf("newConsumerMetrics: %v", err)
	}

	c.metrics = m

	if err := c.AddHandler(testTopic, &mockHandler{}); err != nil {
		b.Fatalf("AddHandler: %v", err)
	}

	return c
}

// BenchmarkProducerRecordSend — учёт исхода отправки: два инструмента на
// сообщение (счётчик + гистограмма).
func BenchmarkProducerRecordSend(b *testing.B) {
	ctx := b.Context()

	for name, meter := range benchMeters() {
		b.Run(name, func(b *testing.B) {
			p := benchProducer(b, meter)

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				p.recordSend(ctx, testTopic, time.Millisecond, nil)
			}
		})
	}
}

// BenchmarkConsumerRecordOutcome — учёт исхода обработки: гистограмма
// длительности плюс счётчик обработанных.
func BenchmarkConsumerRecordOutcome(b *testing.B) {
	ctx := b.Context()

	for name, meter := range benchMeters() {
		b.Run(name, func(b *testing.B) {
			c := benchConsumer(b, meter)

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				c.recordOutcome(ctx, testTopic, consumerStatusSuccess, time.Millisecond)
			}
		})
	}
}

// BenchmarkConsumerCountMessage — счётчик исходов в одиночку: он вызывается и
// с путей, где гистограмму писать нечего (dropped, cancelled).
func BenchmarkConsumerCountMessage(b *testing.B) {
	ctx := b.Context()

	for name, meter := range benchMeters() {
		b.Run(name, func(b *testing.B) {
			c := benchConsumer(b, meter)

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				c.countMessage(ctx, testTopic, consumerStatusDropped)
			}
		})
	}
}

// nopWriter — сток вывода логов.
//
// Не io.Discard: sloglint видит его и требует slog.DiscardHandler, а тот
// возвращает из WithAttrs себя же — то есть ровно ту работу, цену которой
// бенчмарк и меряет, не делает вовсе.
type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

// benchLogger пишет в никуда, но через настоящий JSON-хэндлер: Logger.With
// клонирует именно хэндлер вместе с предформатированными атрибутами, и на
// заглушке измерять было бы нечего.
func benchLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(nopWriter{}, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// benchUseLogger повторяет структуру processRecord: логгер записи готовится на
// каждое сообщение, а пишет в него только путь отказа.
func benchUseLogger(base *slog.Logger, span trace.Span, fail bool) {
	log := &recordLogger{base: base, offset: 42}
	log.span = span

	if fail {
		log.get().Error("Handler failed")
	}
}

// BenchmarkRecordLoggerUnused — happy path: логгер записи построен, но ни одной
// строки не написано. Так выглядят ~100% сообщений в нормальном режиме.
func BenchmarkRecordLoggerUnused(b *testing.B) {
	base := benchLogger()
	span := testSpan(true)

	b.ReportAllocs()

	for range b.N {
		benchUseLogger(base, span, false)
	}
}

// BenchmarkRecordLoggerUsed — путь отказа: логгер записи построен и один раз
// использован. Здесь ленивость обязана не проиграть.
func BenchmarkRecordLoggerUsed(b *testing.B) {
	base := benchLogger()
	span := testSpan(true)

	b.ReportAllocs()

	for range b.N {
		benchUseLogger(base, span, true)
	}
}
