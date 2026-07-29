package kafkax

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

// TestConsumerPoisonedPartitionResumesOnReassignment закрывает разрыв между
// обещанием документации и состоянием клиента.
//
// Отравленная партиция ставится на паузу через PauseFetchPartitions, а набор
// приостановленных партиций в franz-go принадлежит КЛИЕНТУ, а не назначению:
// ребаланс его не трогает, снять паузу могут только методы Resume*. Пока паузу
// не снимал никто, партиция, вернувшаяся к тому же экземпляру, получала свежего
// воркера с poisoned=false, но выключенный фетч — и «сообщение приедет заново
// после ребаланса» было ложью для всех, кроме переезда на другой процесс.
//
// Паузу снимает создание воркера (resumePartition), а не список assigned.
// Балансировщик franz-go по умолчанию кооперативный, и в assigned приходят
// только вновь добавленные партиции: снятие по этому списку промахивалось бы
// мимо всех, кто остался за тем же экземпляром, и наоборот — возвращало бы в
// выборку партицию, чей отравленный воркер жив и продолжает выбрасывать записи.
func TestConsumerPoisonedPartitionResumesOnReassignment(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)

	cfg := testConfig(t, brokers...)
	// Эйджер-балансировщик вместо умолчания (cooperative-sticky) — не прихоть:
	// при кооперативном ребалансе партиция остаётся за прежним участником, и
	// колбэк назначения ему не приходит вовсе, так что сценарий «партиция
	// вернулась» на нём не воспроизводится. RoundRobin отзывает всё и раздаёт
	// заново, поэтому уход второго участника гарантированно оборачивается
	// назначением p0 первому.
	cfg.ExtraOpts = []kgo.Opt{kgo.Balancers(kgo.RoundRobinBalancer())}

	p := mustProducer(t, cfg)
	if err := p.SendMessage(t.Context(), PublishRequest{
		Topic: testTopic,
		Value: []byte("poison"),
	}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	failing := errors.New("обработчик падает всегда")

	handlerA := &mockHandler{returnErr: failing}
	consumerA := mustConsumer(t, cfg)
	mustAddHandler(t, consumerA, testTopic, handlerA)
	consStart(t, consumerA)

	waitFor(t, consWait, "первая доставка отравленного сообщения", func() bool {
		return handlerA.callCount() >= 1
	})

	// Второй участник запускает ребаланс. Кому именно достанется p0, тест не
	// загадывает: важно только, что после ухода второго она вернётся первому.
	handlerB := &mockHandler{returnErr: failing}
	consumerB := mustConsumer(t, cfg)
	mustAddHandler(t, consumerB, testTopic, handlerB)
	consStart(t, consumerB)

	waitFor(t, consWait, "ребаланс отдал партицию одному из участников", func() bool {
		return handlerB.callCount() >= 1 || handlerA.callCount() >= 2
	})

	if err := consumerB.Stop(); err != nil {
		t.Fatalf("Stop второго консьюмера: %v", err)
	}

	waitFor(t, consWait, "сообщение приехало первому консьюмеру заново", func() bool {
		return handlerA.callCount() >= 2
	})
}

// TestPanicInProcessingWrapperPoisonsPartition — паника в обвязке обработки
// останавливает партицию, а не пропускает запись молча.
//
// Обработчик и хук пропуска паникуют «внутрь» своих recover и идут штатным
// путём отказа. Всё остальное в processRecord — спан, логгер, инструменты
// метрик — чужой код, чей recover стоит уже вокруг всей функции. Ему мало
// отрапортовать: вернувшись штатно, processRecord оставила бы запись без
// отметки, но не остановила бы партицию, и первая же успешная запись за ней
// сдвинула бы коммит через необработанную. Это молчаливая потеря данных при
// заявленном at-least-once: метрики зелёные, паника в логе есть, связать её с
// пропавшим сообщением нечем.
func TestPanicInProcessingWrapperPoisonsPartition(t *testing.T) { //nolint:paralleltest // подменяет глобальный MeterProvider
	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)

	prod := consNewProducer(t, brokers)
	prod.send(t, testTopic, 0, "first")
	prod.send(t, testTopic, 0, "second")

	// Паника ровно на первой записи. Паникуй инструмент на каждой, тест не
	// отличил бы отравление от «второй записи тоже не повезло».
	installPanickingHistogram(t)

	h := &mockHandler{}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)
	consStart(t, c)

	waitFor(t, consWait, "первая запись дошла до обработчика", func() bool {
		return consHasValue(h.messages(), "first")
	})

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Обе записи обязаны приехать заново: первую уронила обвязка, вторую не
	// имела права обогнать первую.
	got := consDrainFresh(t, cfg, prod, testTopic, 0)

	want := []string{"first", "second", consMarkerValue}
	if len(got) != len(want) {
		t.Fatalf("свежий консьюмер получил %v, want %v: коммит перепрыгнул запись, "+
			"уронившую обвязку", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("свежий консьюмер получил %v, want %v", got, want)
		}
	}
}

// installPanickingHistogram подменяет глобальный MeterProvider на такой, чей
// Float64Histogram.Record паникует один-единственный раз.
//
// Гистограмма — не произвольный выбор: kafkax.consumer.message.duration
// пишется в processRecord уже после вердикта обработчика и до отметки оффсета,
// то есть ровно в том окне, где паника чужого экспортёра максимально опасна.
func installPanickingHistogram(t *testing.T) {
	t.Helper()

	prev := otel.GetMeterProvider()

	otel.SetMeterProvider(panicOnceMeterProvider{fired: new(atomic.Bool)})
	t.Cleanup(func() { otel.SetMeterProvider(prev) })
}

type panicOnceMeterProvider struct {
	metricnoop.MeterProvider

	fired *atomic.Bool
}

func (p panicOnceMeterProvider) Meter(_ string, _ ...metric.MeterOption) metric.Meter {
	return panicOnceMeter{fired: p.fired}
}

type panicOnceMeter struct {
	metricnoop.Meter

	fired *atomic.Bool
}

func (m panicOnceMeter) Float64Histogram(
	_ string, _ ...metric.Float64HistogramOption,
) (metric.Float64Histogram, error) {
	return panicOnceHistogram{fired: m.fired}, nil
}

type panicOnceHistogram struct {
	metricnoop.Float64Histogram

	fired *atomic.Bool
}

func (h panicOnceHistogram) Record(_ context.Context, _ float64, _ ...metric.RecordOption) {
	if h.fired.CompareAndSwap(false, true) {
		panic("kafkax test: metric exporter blew up")
	}
}
