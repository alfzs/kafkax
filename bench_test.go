package kafkax

import (
	"context"
	"log/slog"
	"sync/atomic"
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
func benchProducer(b *testing.B, meter metric.Meter) *Producer {
	b.Helper()

	p := &Producer{}
	if err := p.initMetrics(meter); err != nil {
		b.Fatalf("initMetrics: %v", err)
	}

	return p
}

// benchConsumer собирает консьюмер без клиента и с зарегистрированным
// обработчиком: набор топиков консьюмера — это набор его обработчиков.
func benchConsumer(b *testing.B, meter metric.Meter) *Consumer {
	b.Helper()

	// Поля повторяют NewConsumer в той части, которой касается путь
	// сообщения: клиент, конфигурация и жизненный контекст здесь не нужны.
	c := &Consumer{opts: newOptsCache(0)}

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

// benchLookupTopics — набор топиков для бенчмарков поиска обработчика.
//
// Их несколько, а не один: карта из единственного ключа — вырожденный случай,
// на котором рантайм Go иногда обходится без полноценного хэширования, и
// измерение перестало бы отвечать на вопрос про реальный консьюмер.
var benchLookupTopics = []string{testTopic, testTopic + "-b", testTopic + "-c", testTopic + "-d"}

// benchLookupSink не даёт компилятору выбросить поиск целиком.
//
// Атомарный счётчик, а не обычная переменная: тело RunParallel исполняется в
// нескольких горутинах, и обычная запись стала бы гонкой — то есть бенчмарк,
// который нельзя прогнать под -race.
var benchLookupSink atomic.Int64

// benchLookupConsumer собирает консьюмер ради одной только карты обработчиков:
// поиск не трогает ни клиента, ни метрики. opts нужен потому, что AddHandler
// прогревает кэш опций.
func benchLookupConsumer(b *testing.B) *Consumer {
	b.Helper()

	c := &Consumer{opts: newOptsCache(0)}

	for _, topic := range benchLookupTopics {
		if err := c.AddHandler(topic, &mockHandler{}); err != nil {
			b.Fatalf("AddHandler(%q): %v", topic, err)
		}
	}

	return c
}

// BenchmarkHandlerLookup — поиск обработчика на пути сообщения.
//
// Обязательно под RunParallel: цена замка здесь не в самом поиске, а в
// атомарном инкременте счётчика читателей RWMutex на кэш-линии, которую
// дёргают все воркеры партиций одновременно. На одном ядре этой цены не видно
// вовсе, и однопоточный бенчмарк показал бы, что менять нечего.
func BenchmarkHandlerLookup(b *testing.B) {
	c := benchLookupConsumer(b)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var found int64

		for pb.Next() {
			if _, ok := c.handler(testTopic); ok {
				found++
			}
		}

		benchLookupSink.Add(found)
	})
}

// BenchmarkHandlerLookupPlainMap — нижняя граница: чтение той же карты вообще
// без синхронизации.
//
// Это не альтернатива, а измерительный эталон. Он отвечает на единственный
// вопрос: осталось ли в handler() что-нибудь сверх чтения карты. Пока
// BenchmarkHandlerLookup держится рядом с ним, синхронизация на горячем пути
// бесплатна; расхождение в разы означает, что замок вернулся.
func BenchmarkHandlerLookupPlainMap(b *testing.B) {
	handlers := make(map[string]ConsumerHandler, len(benchLookupTopics))
	for _, topic := range benchLookupTopics {
		handlers[topic] = &mockHandler{}
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var found int64

		for pb.Next() {
			if _, ok := handlers[testTopic]; ok {
				found++
			}
		}

		benchLookupSink.Add(found)
	})
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

// BenchmarkProducerSendContext — бюджет времени одной отправки.
//
// Не BenchmarkSendMessage, как предлагал аудит: полный SendMessage упирается в
// брокера (~150 µs даже против kfake), и сотня наносекунд обвязки в нём
// неразличима. Здесь меряется ровно то, что правка трогает.
//
// Две ветки принципиально разной цены, поэтому и два подбенчмарка:
//
//   - caller_deadline — у вызывающего дедлайн раньше MessageTimeout, свой
//     контекст не создаётся; сенсор на нулевые аллокации;
//   - no_deadline — общий случай, полный context.WithTimeout. Он здесь как
//     точка отсчёта: без него нулю в первой ветке не с чем сравниться.
func BenchmarkProducerSendContext(b *testing.B) {
	p := &Producer{messageTimeout: 30 * time.Second}

	withDeadline, cancel := context.WithTimeout(b.Context(), time.Second)
	defer cancel()

	for name, ctx := range map[string]context.Context{
		"caller_deadline": withDeadline,
		"no_deadline":     b.Context(),
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			for range b.N {
				_, done := p.sendContext(ctx)
				done()
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
