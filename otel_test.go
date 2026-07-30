package kafkax

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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
//
// Сетки выписаны литералом, а не взяты из consumerDurationBuckets и
// producerDurationBuckets. Через переменные тест сравнивал значения сами с
// собой и пропускал любую их правку: и срезанную верхнюю границу продюсера, и
// выброшенный нижний хвост консьюмера — соседний
// TestDurationBucketsAreSaneGrids обе мутации переживает, монотонность они не
// нарушают. Литерал делает изменение сетки осознанным: правка требуется в двух
// местах, и вторая — здесь, рядом с объяснением, зачем сетка такая
// (TestDurationBucketsExpressTheirRationale).
func TestDurationHistogramsDeclareExplicitBuckets(t *testing.T) { //nolint:paralleltest // подменяет глобальный MeterProvider
	rec := captureMetrics(t)

	// Инструменты регистрируются в конструкторах, и обоим достаточно
	// глобального MeterProvider: до брокера ни один из них не ходит.
	if _, err := newConsumerMetrics(otel.GetMeterProvider().Meter(instrumentationName)); err != nil {
		t.Fatalf("newConsumerMetrics: %v", err)
	}

	p := &Producer{}
	if err := p.initMetrics(otel.GetMeterProvider().Meter(instrumentationName)); err != nil {
		t.Fatalf("initMetrics: %v", err)
	}

	cases := []struct {
		name string
		want []float64
	}{
		{
			"kafkax.consumer.message.duration",
			[]float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300},
		},
		{
			"kafkax.producer.message.duration",
			[]float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		},
	}

	for _, tc := range cases {
		if got := rec.bucketsOf(tc.name); !slices.Equal(got, tc.want) {
			t.Errorf("%s зарегистрирована с границами %v, want %v\n"+
				"сетка изменилась осознанно? обновите литерал и сверьтесь с "+
				"TestDurationBucketsExpressTheirRationale", tc.name, got, tc.want)
		}
	}
}

// TestDurationBucketsExpressTheirRationale — границы сеток держатся не на
// снимке значений, а на причинах, по которым эти значения выбраны.
//
// Литеральный снимок соседнего теста ловит любую правку, но одинаково громко
// кричит и на осмысленную, и на бессмысленную: он знает, что число изменилось,
// и не знает, чему оно должно равняться. Здесь проверяется именно связь с
// причиной — такой ассерт переживает согласованное изменение (вырос бюджет
// доставки — выросла и верхняя граница) и краснеет на рассогласовании, которое
// снимок пропустил бы, будь литерал обновлён «под факт».
func TestDurationBucketsExpressTheirRationale(t *testing.T) {
	t.Parallel()

	t.Run("верхняя граница продюсера равна бюджету доставки", func(t *testing.T) {
		t.Parallel()

		// Превышение MessageTimeout обязано читаться как переполнение
		// последнего бакета. Если верхняя граница ниже бюджета, все медленные
		// отправки уезжают в +Inf и «сколько их было близко к таймауту»
		// перестаёт быть вопросом к гистограмме.
		want := DefaultConfig().Producer.MessageTimeout.Seconds()

		if got := producerDurationBuckets[len(producerDurationBuckets)-1]; got != want {
			t.Errorf("верхняя граница продюсера = %v, want %v (Producer.MessageTimeout по умолчанию)", got, want)
		}
	})

	t.Run("консьюмер размечен от миллисекунд", func(t *testing.T) {
		t.Parallel()

		// Обработчик, отвечающий из памяти, укладывается в доли миллисекунды.
		// Уедет нижняя граница к сотням миллисекунд — весь быстрый трафик
		// сольётся в первый бакет, и p50 станет неотличим от p99: ровно та
		// поломка, от которой явные границы и заводились.
		const want = 0.001

		if got := consumerDurationBuckets[0]; got > want {
			t.Errorf("нижняя граница консьюмера = %v, want <= %v", got, want)
		}
	})

	t.Run("хвост консьюмера длиннее продюсерского", func(t *testing.T) {
		t.Parallel()

		// Консьюмер меряет обработку целиком, вместе с повторами и паузами
		// между ними, продюсер — одну отправку в пределах своего бюджета.
		// Сравнявшиеся хвосты означали бы, что честная долгая обработка с
		// повторами больше не отличается от «зависло навсегда».
		var (
			consumer = consumerDurationBuckets[len(consumerDurationBuckets)-1]
			producer = producerDurationBuckets[len(producerDurationBuckets)-1]
		)

		if consumer <= producer {
			t.Errorf("хвост консьюмера = %v, продюсера = %v: want строго длиннее", consumer, producer)
		}
	})
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

// Тесты кэша опций метрик.
//
// Кэш не наблюдаем снаружи пакета: он не меняет ни имён метрик, ни атрибутов,
// а только цену их записи. Поэтому проверяется двумя способами — что атрибуты
// на выходе те же, что строил metric.WithAttributes, и что кэш остаётся
// ограниченным по памяти на входных данных, которые пакет не контролирует.

// optsAttrs разбирает готовые опции обратно в множество атрибутов — ровно так,
// как это делает SDK на приёме.
func optsAttrs(t *testing.T, opts *metricOpts) attribute.Set {
	t.Helper()

	add := metric.NewAddConfig(opts.add).Attributes()
	rec := metric.NewRecordConfig(opts.record).Attributes()

	if !add.Equals(&rec) {
		t.Fatalf("AddOption и RecordOption разошлись: %v vs %v", add, rec)
	}

	return add
}

// TestOptsCacheAttributesMatchWithAttributes — кэш отдаёт то же множество
// атрибутов, что построил бы metric.WithAttributes на месте.
func TestOptsCacheAttributesMatchWithAttributes(t *testing.T) {
	t.Parallel()

	cache := newOptsCache(8)

	tests := map[string]struct {
		status string
		want   []attribute.KeyValue
	}{
		"topic only": {
			status: noStatus,
			want:   []attribute.KeyValue{attribute.String("topic", testTopic)},
		},
		"topic and status": {
			status: consumerStatusSuccess,
			want: []attribute.KeyValue{
				attribute.String("topic", testTopic),
				attribute.String("status", consumerStatusSuccess),
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := optsAttrs(t, cache.get(testTopic, tt.status))
			want := metric.NewAddConfig([]metric.AddOption{metric.WithAttributes(tt.want...)}).Attributes()

			if !got.Equals(&want) {
				t.Errorf("атрибуты = %v, want %v", got.ToSlice(), want.ToSlice())
			}
		})
	}
}

// TestOptsCacheReusesEntries — повторный запрос отдаёт тот же экземпляр, а не
// собирает опции заново. Это и есть весь смысл кэша, и проверить его иначе,
// чем сравнением указателей, нечем.
func TestOptsCacheReusesEntries(t *testing.T) {
	t.Parallel()

	cache := newOptsCache(4)

	first := cache.get(testTopic, consumerStatusSuccess)
	if second := cache.get(testTopic, consumerStatusSuccess); second != first {
		t.Errorf("повторный get вернул другой экземпляр")
	}

	if other := cache.get(testTopic, consumerStatusError); other == first {
		t.Errorf("разные статусы получили общий набор опций")
	}
}

// TestOptsCacheWarmIgnoresLimit — прогрев укладывает все статусы топика и не
// смотрит на потолок: топики приходят из AddHandler, то есть из кода
// приложения, а не из данных.
func TestOptsCacheWarmIgnoresLimit(t *testing.T) {
	t.Parallel()

	cache := newOptsCache(0)
	cache.warm(testTopic, consumerStatuses...)

	entries := *cache.entries.Load()
	if want := len(consumerStatuses) + 1; len(entries) != want {
		t.Fatalf("записей после прогрева = %d, want %d", len(entries), want)
	}

	// После прогрева путь сообщения обязан находить готовое: если бы он
	// промахивался, кэш с limit=0 собирал бы опции заново на каждое сообщение.
	for _, status := range append([]string{noStatus}, consumerStatuses...) {
		if got := cache.get(testTopic, status); got != entries[optKey{topic: testTopic, status: status}] {
			t.Errorf("get(%q) промахнулся мимо прогретой записи", status)
		}
	}

	// Повторный прогрев того же топика не должен ни ломать записи, ни плодить
	// новые: AddHandler дубликаты отвергает, но кэш не обязан на это полагаться.
	cache.warm(testTopic, consumerStatuses...)

	if got := len(*cache.entries.Load()); got != len(entries) {
		t.Errorf("записей после повторного прогрева = %d, want %d", got, len(entries))
	}
}

// TestOptsCacheStopsGrowingAtLimit — кэш не растёт за потолок.
//
// Это проверка не производительности, а памяти: topic продюсера приходит из
// PublishRequest и ничем не ограничен, так что кэш без потолка был бы утечкой,
// растущей ровно со скоростью подстановки пользовательского ввода в топик.
func TestOptsCacheStopsGrowingAtLimit(t *testing.T) {
	t.Parallel()

	const limit = 4

	cache := newOptsCache(limit)

	for i := range 100 {
		topic := fmt.Sprintf("topic-%d", i)

		// Опции остаются годными и за потолком — просто собираются на месте.
		attrs := optsAttrs(t, cache.get(topic, statusSuccess))
		if got := attrs.Len(); got != 2 {
			t.Fatalf("атрибутов у топика %q = %d, want 2", topic, got)
		}
	}

	if got := len(*cache.entries.Load()); got != limit {
		t.Errorf("записей в кэше = %d, want %d", got, limit)
	}

	// За потолком кэш ничего не запоминает: каждый вызов строит опции заново.
	beyond := "topic-99"

	first := cache.get(beyond, statusSuccess)
	if second := cache.get(beyond, statusSuccess); second == first {
		t.Errorf("топик за потолком всё-таки осел в кэше")
	}
}

// TestOptsCacheConcurrentGet — параллельные промахи по одному ключу.
//
// Смысл не в скорости, а в двух инвариантах под -race: снимок карты
// публикуется атомарно, а повторная проверка под мьютексом не даёт двум
// промахам разложить в карту две записи для одного набора атрибутов.
func TestOptsCacheConcurrentGet(t *testing.T) {
	t.Parallel()

	const goroutines = 16

	cache := newOptsCache(goroutines)
	got := make([]*metricOpts, goroutines)

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
	)

	for i := range goroutines {
		wg.Go(func() {
			<-start

			got[i] = cache.get(testTopic, consumerStatusSuccess)
		})
	}

	close(start)
	wg.Wait()

	for i, opts := range got {
		if opts != got[0] {
			t.Fatalf("горутина %d получила другой экземпляр опций", i)
		}
	}

	if n := len(*cache.entries.Load()); n != 1 {
		t.Errorf("записей в кэше = %d, want 1", n)
	}
}
