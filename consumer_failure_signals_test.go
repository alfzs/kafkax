package kafkax

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
)

// Сигналы отказа: что именно консьюмер сообщает наружу, когда обработка пошла
// не так, и чего он при этом НЕ сообщает.
//
// Тема файла — не «отказ обработан», а «отказ читаем». Три класса дефекта,
// каждый из которых не ломает ни одного функционального теста и потому живёт в
// коде годами: контекст, потерянный на границе пакета; счётчик, растущий не по
// событиям, а по времени; и фон Error при полностью исправной работе. Все три
// одинаково кончаются тем, что на сигнал перестают реагировать.

// TestBaggageReachesHandler — baggage отправителя доезжает до обработчика.
//
// Класс дефекта: контекст, потерянный на границе библиотеки. kotel извлекает
// propagator'ом весь контекст записи — и trace context, и baggage — в
// rec.Context, откуда WithProcessSpan отдаёт его первым результатом. Пакет этот
// результат отбрасывал: обработчику доставался контекст воркера, куда переносился
// только сам спан. Отмена так работала правильно (она живёт в контексте воркера,
// и менять эту основу нельзя), а tenant_id, request_id и всё прочее, чем
// вызывающая сторона размечает запрос, обрывалось ровно здесь.
//
// Незаметность дефекта в том, что трассировка при этом выглядела целой: спаны
// сшивались, потому что trace context переносится вместе со спаном. Отсюда
// ассерт именно на baggage, а не на TraceID: сломайся перенос снова, спаны
// по-прежнему сойдутся, и заметить будет нечем.
//
//nolint:paralleltest // подменяет глобальный TextMapPropagator: параллельный сосед увидел бы чужой
func TestBaggageReachesHandler(t *testing.T) {
	const (
		topic  = "kafkax-baggage-topic"
		member = "tenant_id"
		value  = "acme"
	)

	// Propagator глобальный и по умолчанию no-op: без baggage в наборе заголовок
	// не запишется на отправке и не прочитается на приёме, и тест доказывал бы
	// только то, что пустое равно пустому.
	prev := otel.GetTextMapPropagator()

	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	cfg := testConfig(t, newFakeCluster(t, 1, topic)...)

	// Продюсер здесь именно публичный, а не сырой клиент franz-go из остальных
	// сценариев: заголовок baggage пишет kotel на хуке отправки, и без него
	// тест проверял бы приём того, чего никто не отправлял.
	prod := mustProducer(t, cfg)

	bagMember, err := baggage.NewMember(member, value)
	if err != nil {
		t.Fatalf("baggage.NewMember: %v", err)
	}

	bag, err := baggage.New(bagMember)
	if err != nil {
		t.Fatalf("baggage.New: %v", err)
	}

	seen := make(chan string, 1)
	h := ConsumerHandlerFunc(func(ctx context.Context, _ IncomingMessage) error {
		select {
		case seen <- baggage.FromContext(ctx).Member(member).Value():
		default:
		}

		return nil
	})

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	// Baggage кладётся в контекст отправки: продюсер инжектит его в заголовки
	// тем же глобальным propagator'ом, что и trace context.
	sendCtx := baggage.ContextWithBaggage(t.Context(), bag)
	if err := prod.SendMessage(sendCtx, PublishRequest{Topic: topic, Value: []byte("payload")}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case got := <-seen:
		if got != value {
			t.Fatalf("baggage[tenant_id] = %q, want %q: контекст kotel потерян на границе пакета", got, value)
		}
	case <-time.After(consWait):
		t.Fatal("обработчик не вызван")
	}
}

// TestInfiniteRetriesReportPanicOnce — при HandlerRetries=-1 паника
// обработчика рапортуется один раз на сообщение, а не один раз на попытку.
//
// Класс дефекта: счётчик, растущий по времени, а не по событиям. Конфигурация
// «повторять бесконечно» рекомендована в doc.go как одна из трёх осмысленных, и
// на детерминированно паникующем сообщении цикл не кончается никогда. Рапорт на
// каждой попытке давал бы полный стек в лог с частотой повторов и линейно
// растущий kafkax.consumer.panics — по такому счётчику «одно сообщение крутится
// сутки» неотличимо от «упало N разных сообщений», то есть ни алерт построить,
// ни инцидент оценить.
//
// Проверяется пара утверждений, а не одно: счётчик паник стоит на единице, а
// счётчик повторов при этом растёт. Без второго ассерт прошёл бы и в мире, где
// повторов не осталось вовсе, — а это уже другой контракт, ломающий at-least-once
// ожидания тех, кто ставил -1 осознанно.
//
//nolint:paralleltest // подменяет глобальный MeterProvider
func TestInfiniteRetriesReportPanicOnce(t *testing.T) {
	const (
		topic       = "kafkax-infinite-retry-topic"
		wantRetries = 5
	)

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, topic)
	cfg := testConfig(t, brokers...)
	cfg.Consumer.HandlerRetries = -1
	cfg.Consumer.HandlerRetryDelay = time.Millisecond
	// Воркер сидит в бесконечном цикле повторов и очередь не читает: выйти он
	// может только по жёсткой отмене, а до неё Stop ждёт весь GracefulTimeout.
	cfg.GracefulTimeout = 200 * time.Millisecond

	sites := &consTrace{}
	cfg.OnPanic = func(_ context.Context, site PanicSite, _ any, _ []byte) {
		sites.add(string(site))
	}

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "boom")

	h := &mockHandler{fn: func(int, IncomingMessage) error {
		panic("handler exploded")
	}}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	waitFor(t, consWait, "обработчик перевызван много раз", func() bool {
		return rec.sum(consMetricRetries, attribute.String("topic", topic)) >= wantRetries
	})

	if got := rec.sum(consMetricPanics, attribute.String("site", string(PanicSiteHandler))); got != 1 {
		t.Fatalf("panics(site=%s) = %d, want 1 при %d+ повторах: рапорт пишется на попытку, а не на сообщение",
			PanicSiteHandler, got, wantRetries)
	}

	// OnPanic — тот же рапорт, только чужой код: зваться он обязан столько же раз.
	if got := len(sites.snapshot()); got != 1 {
		t.Fatalf("OnPanic вызван %d раз, want 1", got)
	}

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestSkipHookSuccessLogsNoError — штатный пропуск через OnMessageSkipped не
// пишет ни одной записи уровня Error.
//
// Класс дефекта: фон Error при исправной работе. Конфигурация с работающим
// DLQ-хуком — рекомендованная (doc.go), и её нормальный исход выглядел так:
// runHandler писал Error «Handler failed, giving up», после чего resolveFailure
// писал Warn о том, что сообщение штатно забрано. То есть каждое отправленное в
// DLQ сообщение поднимало Error, и это ровно та причина, по которой на Error в
// проде перестают смотреть.
//
// Отсюда ассерт на уровень записей, а не на их текст: важно не какая строка
// исчезла, а что уровень выбирает тот единственный слой, который знает исход.
// Заодно фиксируется вторая половина того же решения — отравление партиции
// по-прежнему Error, и ровно одной записью, а не тремя на трёх уровнях стека.
func TestSkipHookSuccessLogsNoError(t *testing.T) {
	t.Parallel()

	const topic = "kafkax-skip-quiet-topic"

	levels := &levelCount{}

	brokers := newFakeCluster(t, 1, topic)
	cfg := testConfig(t, brokers...)
	cfg.Logger = slog.New(&levelCountHandler{inner: cfg.Logger.Handler(), count: levels})

	skipped := make(chan struct{}, 1)
	cfg.OnMessageSkipped = func(context.Context, IncomingMessage, error) error {
		select {
		case skipped <- struct{}{}:
		default:
		}

		return nil
	}

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "boom")

	h := &mockHandler{returnErr: errConsBoom}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	select {
	case <-skipped:
	case <-time.After(consWait):
		t.Fatal("OnMessageSkipped не вызван")
	}

	// Хук отработал синхронно внутри processRecord, и все записи об отказе уже
	// в журнале: после Stop к ним ничего относящегося к отказу не добавится.
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if got := levels.of(slog.LevelError); got != 0 {
		t.Fatalf("записей уровня Error: %d, want 0 — штатный пропуск не событие уровня Error", got)
	}

	// Warn ровно один: сам пропуск. Повторов нет (HandlerRetries=0), значит
	// и «Handler failed, retrying» быть не должно.
	if got := levels.of(slog.LevelWarn); got != 1 {
		t.Fatalf("записей уровня Warn: %d, want 1 (только сам пропуск)", got)
	}
}

// TestPoisonLogsSingleError — отравление партиции даёт ровно одну запись Error.
//
// Обратная половина предыдущего теста: убрав Error из runHandler, легко убрать
// его совсем. Здесь хука нет, сообщение никуда не уходит, партиция встаёт — и
// это как раз тот случай, когда Error обязателен. Одна запись, а не три:
// причина отказа, число попыток и ветка resolveFailure собраны в неё атрибутами.
func TestPoisonLogsSingleError(t *testing.T) {
	t.Parallel()

	const topic = "kafkax-poison-quiet-topic"

	levels := &levelCount{}

	brokers := newFakeCluster(t, 1, topic)
	cfg := testConfig(t, brokers...)
	cfg.Logger = slog.New(&levelCountHandler{inner: cfg.Logger.Handler(), count: levels})
	cfg.Consumer.HandlerRetries = 2

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "boom")

	h := &mockHandler{returnErr: errConsBoom}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	waitFor(t, consWait, "обработчик исчерпал попытки", func() bool {
		return h.callCount() >= 3
	})

	waitFor(t, consWait, "партиция отравлена и записан Error", func() bool {
		return levels.of(slog.LevelError) >= 1
	})

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if got := levels.of(slog.LevelError); got != 1 {
		t.Fatalf("записей уровня Error: %d, want 1 — один отказ, одна запись", got)
	}

	// Два повтора — два Warn, и ни одного лишнего.
	if got := levels.of(slog.LevelWarn); got != 2 {
		t.Fatalf("записей уровня Warn: %d, want 2 (по одной на повтор)", got)
	}
}

// levelCount — счётчик записей по уровням.
type levelCount struct {
	mu sync.Mutex
	by map[slog.Level]int
}

func (c *levelCount) add(level slog.Level) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.by == nil {
		c.by = make(map[slog.Level]int)
	}

	c.by[level]++
}

func (c *levelCount) of(level slog.Level) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.by[level]
}

// levelCountHandler считает записи по уровням, пропуская их дальше.
//
// Считаются только записи самого пакета: логгер franz-go строится от того же
// базового, но проходит через kslog, который добавляет собственную группу
// атрибутов, — а тесты выше отправляют записи в топик, где franz-go на уровне
// Info и выше молчит обо всём, кроме жизненного цикла группы. Именно поэтому
// ассерты стоят на Error и Warn, а не на Info: последний принадлежит не только
// пакету.
type levelCountHandler struct {
	inner slog.Handler
	count *levelCount
}

func (h *levelCountHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *levelCountHandler) Handle(ctx context.Context, record slog.Record) error {
	h.count.add(record.Level)

	return h.inner.Handle(ctx, record)
}

func (h *levelCountHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &levelCountHandler{inner: h.inner.WithAttrs(attrs), count: h.count}
}

func (h *levelCountHandler) WithGroup(name string) slog.Handler {
	return &levelCountHandler{inner: h.inner.WithGroup(name), count: h.count}
}
