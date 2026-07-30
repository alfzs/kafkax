package kafkax

import (
	"context"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// Настройка kotel: что пакет сообщает трейсеру о роли клиента.
//
// consumerGroupKey — ключ, под которым kotel кладёт группу в спаны
// receive/process (semconv v1.18.0). Литералом, а не через semconv: версия
// соглашений принадлежит kotel, и её обновление обязано быть видно здесь
// падением, а не молча переехать вместе с зависимостью.
const consumerGroupKey = attribute.Key("messaging.kafka.consumer.group")

// attrCaptureTracer — трейсер, запоминающий атрибуты старта спанов.
//
// Свой, а не recordingTracer из recorders_test.go: тому атрибуты старта не
// нужны — он считает RecordError и SetStatus, — а расширять общую заглушку
// ради одного файла значило бы платить за это всем остальным тестам.
type attrCaptureTracer struct {
	tracenoop.Tracer

	mu    sync.Mutex
	attrs []attribute.KeyValue
}

func (t *attrCaptureTracer) Start(
	ctx context.Context, _ string, opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	t.mu.Lock()
	defer t.mu.Unlock()

	cfg := trace.NewSpanStartConfig(opts...)
	t.attrs = append(t.attrs, cfg.Attributes()...)

	return ctx, tracenoop.Span{}
}

type attrCaptureTracerProvider struct {
	tracenoop.TracerProvider

	tracer *attrCaptureTracer
}

func (p attrCaptureTracerProvider) Tracer(_ string, _ ...trace.TracerOption) trace.Tracer {
	return p.tracer
}

// spanAttrsOfFetch прогоняет одну входящую запись через трейсер, собранный из
// opts, и отдаёт атрибуты заведённого спана.
//
// Провайдер добавляется последней опцией, а не подменой глобального: опции
// kotel применяются по порядку, последняя побеждает. Так тест не трогает
// процессные глобали OTel и остаётся параллельным.
func spanAttrsOfFetch(opts []kotel.TracerOpt) []attribute.KeyValue {
	capture := &attrCaptureTracer{}
	tracer := kotel.NewTracer(append(opts, kotel.TracerProvider(attrCaptureTracerProvider{tracer: capture}))...)

	rec := &kgo.Record{Topic: testTopic, Context: context.Background()}
	tracer.OnFetchRecordBuffered(rec)

	capture.mu.Lock()
	defer capture.mu.Unlock()

	return capture.attrs
}

// TestTracerOptsCarryConsumerGroup — группа доезжает до спанов консьюмера, и
// только консьюмера.
//
// Половина про непустую группу проверяет, что опция вообще что-то значит:
// messaging.kafka.consumer.group — то, по чему в трассировке отличают
// параллельные группы, читающие один топик.
//
// Половина про пустую группу проверяется по составу списка опций, а не по
// готовому трейсеру, и это не лень: kotel.ConsumerGroup("") записывает в
// неэкспортное поле ту же пустую строку, которая там уже лежит, а атрибут
// добавляется по условию group != "" на стороне kotel. То есть «опции не было»
// и «опция была с пустым значением» на выходе неразличимы, и единственное
// место, где условие в tracerOpts вообще наблюдаемо, — длина списка. Условие
// при этом не лишнее: оно не даёт пакету утверждать факт, которого нет, если
// kotel однажды перестанет отбрасывать пустую группу сам.
func TestTracerOptsCarryConsumerGroup(t *testing.T) {
	t.Parallel()

	const group = "kafkax-telemetry-group"

	t.Run("группа консьюмера доезжает в спан", func(t *testing.T) {
		t.Parallel()

		var got attribute.Value

		for _, attr := range spanAttrsOfFetch(tracerOpts("client", group)) {
			if attr.Key == consumerGroupKey {
				got = attr.Value
			}
		}

		if got.AsString() != group {
			t.Errorf("%s = %q, want %q", consumerGroupKey, got.AsString(), group)
		}
	})

	t.Run("у продюсера группы нет", func(t *testing.T) {
		t.Parallel()

		withGroup := tracerOpts("client", group)
		withoutGroup := tracerOpts("client", "")

		// Базовый состав — литералом: без него «на одну опцию меньше»
		// удовлетворялось бы выбрасыванием любой другой опции.
		if want := 3; len(withoutGroup) != want {
			t.Fatalf("опций при пустой группе = %d, want %d", len(withoutGroup), want)
		}

		if len(withGroup) != len(withoutGroup)+1 {
			t.Errorf("опций с группой = %d, без группы = %d: want ровно на одну больше",
				len(withGroup), len(withoutGroup))
		}
	})
}
