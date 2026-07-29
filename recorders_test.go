package kafkax

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// Записывающие реализации OTel-интерфейсов для тестов метрик и трейсинга.
//
// Собственные заглушки, а не go.opentelemetry.io/otel/sdk: SDK не нужен
// библиотеке в рантайме, и тащить его в go.mod ради проверки двух счётчиков —
// плохой размен. Обе заглушки надстраиваются над noop-реализациями, поэтому
// переопределять приходится только те методы, значения которых проверяются.

// counterAdd — один вызов Int64Counter.Add.
type counterAdd struct {
	name  string
	value int64
	attrs attribute.Set
}

// histogramRecord — один вызов Float64Histogram.Record.
type histogramRecord struct {
	name  string
	value float64
	attrs attribute.Set
}

// recordedMetrics — потокобезопасный журнал вызовов Add/Record: инструменты
// дёргаются из горутин воркеров, а проверяет их тест из своей.
type recordedMetrics struct {
	mu      sync.Mutex
	adds    []counterAdd
	records []histogramRecord
}

func (r *recordedMetrics) recordAdd(name string, value int64, attrs attribute.Set) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.adds = append(r.adds, counterAdd{name: name, value: value, attrs: attrs})
}

func (r *recordedMetrics) recordHistogram(name string, value float64, attrs attribute.Set) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.records = append(r.records, histogramRecord{name: name, value: value, attrs: attrs})
}

// sum складывает значения Add по инструменту name, оставляя только записи, у
// которых совпали все перечисленные атрибуты (лишние атрибуты не мешают).
func (r *recordedMetrics) sum(name string, want ...attribute.KeyValue) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	var total int64

	for _, add := range r.adds {
		if add.name == name && hasAttrs(add.attrs, want) {
			total += add.value
		}
	}

	return total
}

// observations возвращает значения, записанные в гистограмму name и подходящие
// под want.
func (r *recordedMetrics) observations(name string, want ...attribute.KeyValue) []float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []float64

	for _, rec := range r.records {
		if rec.name == name && hasAttrs(rec.attrs, want) {
			out = append(out, rec.value)
		}
	}

	return out
}

// hasAttrs сравнивает значения через Type/String, а не оператором ==:
// attribute.Value содержит поле-интерфейс, и прямое сравнение паникует на
// слайсовых значениях.
func hasAttrs(set attribute.Set, want []attribute.KeyValue) bool {
	for _, kv := range want {
		got, ok := set.Value(kv.Key)
		if !ok || got.Type() != kv.Value.Type() || got.String() != kv.Value.String() {
			return false
		}
	}

	return true
}

// recordingMeterProvider выдаёт recordingMeter на любой scope.
type recordingMeterProvider struct {
	metricnoop.MeterProvider

	rec *recordedMetrics
}

func (p recordingMeterProvider) Meter(_ string, _ ...metric.MeterOption) metric.Meter {
	return recordingMeter{rec: p.rec}
}

// recordingMeter — Meter, считающий Int64Counter'ы и Float64Histogram'ы;
// остальные инструменты остаются noop.
type recordingMeter struct {
	metricnoop.Meter

	rec *recordedMetrics
}

func (m recordingMeter) Int64Counter(name string, _ ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return recordingCounter{rec: m.rec, name: name}, nil
}

func (m recordingMeter) Float64Histogram(
	name string, _ ...metric.Float64HistogramOption,
) (metric.Float64Histogram, error) {
	return recordingHistogram{rec: m.rec, name: name}, nil
}

type recordingCounter struct {
	metricnoop.Int64Counter

	rec  *recordedMetrics
	name string
}

func (c recordingCounter) Add(_ context.Context, incr int64, opts ...metric.AddOption) {
	c.rec.recordAdd(c.name, incr, metric.NewAddConfig(opts).Attributes())
}

type recordingHistogram struct {
	metricnoop.Float64Histogram

	rec  *recordedMetrics
	name string
}

func (h recordingHistogram) Record(_ context.Context, value float64, opts ...metric.RecordOption) {
	h.rec.recordHistogram(h.name, value, metric.NewRecordConfig(opts).Attributes())
}

// recordingSpan перехватывает RecordError/SetStatus — единственные методы
// спана, которые библиотека вызывает сама. Под -race это ещё и проверка, что
// вызовы приходят из разных горутин корректно; геттеры к errs/statusCode
// появятся вместе с тестами трейсинга, которые начнут их читать.
type recordingSpan struct {
	tracenoop.Span

	mu         sync.Mutex
	errs       []error
	statusCode codes.Code
	statusDesc string
}

func (s *recordingSpan) RecordError(err error, _ ...trace.EventOption) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.errs = append(s.errs, err)
}

func (s *recordingSpan) SetStatus(code codes.Code, description string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.statusCode = code
	s.statusDesc = description
}

// recordingTracer выдаёт recordingSpan на каждый Start и хранит все выданные.
type recordingTracer struct {
	tracenoop.Tracer

	mu    sync.Mutex
	spans []*recordingSpan
}

func (t *recordingTracer) Start(
	ctx context.Context, _ string, _ ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	span := &recordingSpan{}

	t.mu.Lock()
	t.spans = append(t.spans, span)
	t.mu.Unlock()

	return ctx, span
}

// started возвращает снимок выданных спанов.
func (t *recordingTracer) started() []*recordingSpan {
	t.mu.Lock()
	defer t.mu.Unlock()

	return append([]*recordingSpan(nil), t.spans...)
}
