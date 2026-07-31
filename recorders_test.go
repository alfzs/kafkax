package kafkax

import (
	"context"
	"maps"
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

// instrumentInfo — вид инструмента и его единица измерения, снятые в момент
// регистрации.
//
// И то и другое — часть контракта наблюдаемости наравне с именем: гейдж,
// ставший счётчиком, ломает алерт «стоит хотя бы одна партиция», а
// длительность, объявленная не в секундах, разъезжается с границами бакетов.
// Ни того ни другого не видно по вызовам Add/Record, поэтому снимается здесь.
type instrumentInfo struct {
	kind string
	unit string
}

// Виды инструментов OTel, которые заводит пакет.
const (
	kindCounter       = "Int64Counter"
	kindUpDownCounter = "Int64UpDownCounter"
	kindHistogram     = "Float64Histogram"
)

// recordedMetrics — потокобезопасный журнал вызовов Add/Record: инструменты
// дёргаются из горутин воркеров, а проверяет их тест из своей.
type recordedMetrics struct {
	mu      sync.Mutex
	adds    []counterAdd
	records []histogramRecord

	// buckets — границы бакетов, с которыми регистрировалась гистограмма.
	// Опции инструмента иначе нигде не видны: значения Record о разметке
	// ничего не говорят, а до реального SDK они не доезжают вовсе.
	buckets map[string][]float64

	// instruments — все зарегистрированные инструменты по именам.
	instruments map[string]instrumentInfo
}

// noteInstrument запоминает факт регистрации инструмента.
func (r *recordedMetrics) noteInstrument(name, kind, unit string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.instruments == nil {
		r.instruments = make(map[string]instrumentInfo)
	}

	r.instruments[name] = instrumentInfo{kind: kind, unit: unit}
}

// registered возвращает снимок зарегистрированных инструментов.
func (r *recordedMetrics) registered() map[string]instrumentInfo {
	r.mu.Lock()
	defer r.mu.Unlock()

	return maps.Clone(r.instruments)
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

func (m recordingMeter) Int64Counter(
	name string, opts ...metric.Int64CounterOption,
) (metric.Int64Counter, error) {
	m.rec.noteInstrument(name, kindCounter, metric.NewInt64CounterConfig(opts...).Unit())

	return recordingCounter{rec: m.rec, name: name}, nil
}

// Int64UpDownCounter пишет в тот же журнал, что и монотонный счётчик: у обоих
// инструментов наблюдаемое событие — вызов Add(delta), и разделять их значило бы
// заводить второй sum с той же логикой. Текущее значение гейджа — это сумма
// дельт, которую sum и считает; отрицательные дельты складываются наравне с
// положительными.
func (m recordingMeter) Int64UpDownCounter(
	name string, opts ...metric.Int64UpDownCounterOption,
) (metric.Int64UpDownCounter, error) {
	m.rec.noteInstrument(name, kindUpDownCounter, metric.NewInt64UpDownCounterConfig(opts...).Unit())

	return recordingUpDownCounter{rec: m.rec, name: name}, nil
}

func (m recordingMeter) Float64Histogram(
	name string, opts ...metric.Float64HistogramOption,
) (metric.Float64Histogram, error) {
	cfg := metric.NewFloat64HistogramConfig(opts...)

	m.rec.noteInstrument(name, kindHistogram, cfg.Unit())

	m.rec.mu.Lock()
	defer m.rec.mu.Unlock()

	if m.rec.buckets == nil {
		m.rec.buckets = make(map[string][]float64)
	}

	m.rec.buckets[name] = cfg.ExplicitBucketBoundaries()

	return recordingHistogram{rec: m.rec, name: name}, nil
}

// bucketsOf возвращает границы, с которыми регистрировалась гистограмма.
func (r *recordedMetrics) bucketsOf(name string) []float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.buckets[name]
}

type recordingCounter struct {
	metricnoop.Int64Counter

	rec  *recordedMetrics
	name string
}

func (c recordingCounter) Add(_ context.Context, incr int64, opts ...metric.AddOption) {
	c.rec.recordAdd(c.name, incr, metric.NewAddConfig(opts).Attributes())
}

type recordingUpDownCounter struct {
	metricnoop.Int64UpDownCounter

	rec  *recordedMetrics
	name string
}

func (c recordingUpDownCounter) Add(_ context.Context, incr int64, opts ...metric.AddOption) {
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
// вызовы приходят из разных горутин корректно.
//
// Журнал именно накопительный: важен не факт записи ошибки, а их число.
// Контракт пакета — одна запись на отказ сообщения независимо от числа
// повторов, и хранение «последней ошибки» скрыло бы ровно ту поломку, ради
// которой заглушка заведена.
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

// recordedErrs возвращает снимок ошибок, попавших в спан.
//
// Снимок, а не сам слайс: спан продолжает жить после чтения, и отданный наружу
// slice header читался бы тестом, пока воркер дописывает в него под -race.
func (s *recordingSpan) recordedErrs() []error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]error(nil), s.errs...)
}

// status возвращает последний установленный статус спана.
func (s *recordingSpan) status() (codes.Code, string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.statusCode, s.statusDesc
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

// recordedErrs собирает ошибки со всех выданных спанов.
//
// Считать приходится по всему трейсеру, а не по одному спану: на каждую запись
// kotel заводит и receive-, и process-спан, и тест не знает заранее, какой из
// них библиотека выберет для отметки об отказе. Утверждение при этом остаётся
// строгим — суммарное число записей об ошибке и есть то, что увидит человек,
// открывший трейс.
func (t *recordingTracer) recordedErrs() []error {
	var out []error

	for _, span := range t.started() {
		out = append(out, span.recordedErrs()...)
	}

	return out
}

// erroredSpans возвращает спаны, чей статус выставлен в codes.Error.
func (t *recordingTracer) erroredSpans() []*recordingSpan {
	var out []*recordingSpan

	for _, span := range t.started() {
		if code, _ := span.status(); code == codes.Error {
			out = append(out, span)
		}
	}

	return out
}

// recordingTracerProvider отдаёт один и тот же записывающий трейсер на любой
// scope. Скоуп спанов принадлежит kotel, и различать его в тестах нечем и
// незачем: пакет своих спанов не заводит вовсе.
type recordingTracerProvider struct {
	tracenoop.TracerProvider

	tracer *recordingTracer
}

func (p recordingTracerProvider) Tracer(_ string, _ ...trace.TracerOption) trace.Tracer {
	return p.tracer
}
