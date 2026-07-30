package kafkax

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
)

// Смерть партиционного воркера и её последствия для цикла опроса.
//
// Паника обработчика — сценарий безобидный: её ловит callHandler, сообщение
// идёт политикой повторов, воркер жив. Здесь проверяется другое: паника в самой
// горутине воркера, вне processRecord. Её ловит внешний recover, после которого
// воркера больше нет, — и дальше начинается то, ради чего этот файл написан:
// партиция замолкает, а цикл опроса продолжает класть в её очередь батчи,
// которые никто не разберёт.
//
// Отказ такого рода не виден ни по ошибкам, ни по трафику: сообщений просто
// нет. Поэтому каждый тест здесь проверяет не «процесс не упал», а конкретный
// след — метрику panics с нужным site, хук OnPanic, гейдж активных воркеров — и
// судьбу самой партиции.

// TestPartitionWorkerPanicIsReportedAndPartitionGoesSilent — паника в теле
// воркера убивает воркера, но обязана оставить машиночитаемый след.
//
// Класс дефекта: непойманная паника в чужой горутине. Без recover она уносит
// весь процесс — вызывающий её не ловит; с recover уносит только партицию, и
// это молчание опаснее падения, потому что лаг растёт, а ошибок нет. Отсюда три
// ассерта на след вместо одного: метрика с site=partition_worker (по ней
// строится алерт), хук OnPanic со стеком (по нему разбираются постфактум) и
// гейдж workers.active, вернувшийся к нулю. Последний проверяет не наблюдаемость,
// а порядок defer'ов: −1 стоит ПОСЛЕ recover, и если его когда-нибудь переставят
// выше, мониторинг будет вечно считать мёртвого воркера живым.
//
// Убивает воркера паникующий slog.Handler, а не обработчик: обработчик падает
// внутрь callHandler и до этого recover не доходит вовсе. Отладочная запись о
// старте воркера — единственное место в runPartitionWorker, где исполняется
// чужой код вне processRecord, и потому единственный способ довести воркера до
// смерти, не отравив партицию по дороге.
//
//nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider: параллельный сосед смешал бы записи
func TestPartitionWorkerPanicIsReportedAndPartitionGoesSilent(t *testing.T) {
	const topic = "kafkax-worker-panic-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, topic)
	watch := &pollWatch{}

	cfg := testConfig(t, brokers...)
	cfg.Logger = newWedgeLogger(t, topic)
	cfg.ExtraOpts = []kgo.Opt{kgo.WithHooks(watch)}

	sites := &consTrace{}
	cfg.OnPanic = func(_ context.Context, site PanicSite, _ any, stack []byte) {
		if len(stack) == 0 {
			sites.add("empty-stack")

			return
		}

		sites.add(string(site))
	}

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "unread")

	h := &mockHandler{}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	waitFor(t, consWait, "паника воркера доложена в OnPanic", func() bool {
		return slices.Contains(sites.snapshot(), string(PanicSitePartitionWorker))
	})

	if got := rec.sum(consMetricPanics, attribute.String("site", string(PanicSitePartitionWorker))); got != 1 {
		t.Fatalf("panics(site=%s) = %d, want 1", PanicSitePartitionWorker, got)
	}

	waitFor(t, consWait, "гейдж активных воркеров вернулся к нулю", func() bool {
		return rec.sum(consMetricWorkers) == 0
	})

	// Запись доехала до цикла опроса — и осталась лежать в очереди мёртвого
	// воркера. Хук franz-go срабатывает внутри PollRecords, поэтому проверка
	// «обработчик не вызван» ниже сравнивает не с пустотой, а с записью,
	// которую консьюмер точно получил.
	waitFor(t, consWait, "цикл опроса вынул запись из буфера клиента", func() bool {
		return watch.polled(topic) >= 1
	})

	if got := h.callCount(); got != 0 {
		t.Fatalf("обработчик вызван %d раз, want 0: воркер мёртв, разбирать очередь некому", got)
	}

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Партиция замолчала, но ничего не потеряла: оффсет не отмечен, и запись
	// приедет следующему владельцу. Свежему консьюмеру логгер-убийца не нужен —
	// иначе он положил бы и его воркера, а тест проверял бы собственную оснастку.
	clean := cfg
	clean.Logger = testLogger(t)

	got := consDrainFresh(t, clean, prod, topic, 0)
	if len(got) != 2 || got[0] != "unread" || got[1] != consMarkerValue {
		t.Fatalf("свежий консьюмер получил %v, want [unread %s]: запись потеряна вместе с воркером",
			got, consMarkerValue)
	}
}

// TestDeadPartitionWorkerDoesNotStallPollLoop — цикл опроса не встаёт на
// партиции, воркер которой умер.
//
// Самый дорогой класс дефекта в этом файле. Отправка батча воркеру блокирующая
// и без таймаута (выброшенный батч был бы перепрыгнут коммитом следующего),
// поэтому ветка case <-worker.done в dispatch — единственное, что отличает
// «партиция замолчала» от «замолчал весь экземпляр»: очередь мёртвого воркера
// не разбирает никто, и без этой ветки опрос повис бы на ней навсегда, унося с
// собой все остальные партиции. Ни ошибки, ни падения при этом нет — только
// растущий лаг.
//
// Отсюда конструкция: очередь ёмкостью в один батч и две записи, доставленные
// РАЗНЫМИ опросами. Первая занимает очередь навсегда, на второй dispatch обязан
// выбрать <-worker.done. Момент, когда вторая запись вынута из буфера клиента,
// тест ловит хуком franz-go — он срабатывает внутри PollRecords, до dispatch,
// поэтому дальше работает доказательство от противного: сообщение живого топика
// доедет до обработчика только в том случае, если цикл опроса пережил отправку
// в мёртвого воркера.
func TestDeadPartitionWorkerDoesNotStallPollLoop(t *testing.T) {
	t.Parallel()

	const (
		wedgeTopic = "kafkax-wedge-topic"
		liveTopic  = "kafkax-live-topic"
	)

	brokers := newFakeCluster(t, 1, wedgeTopic, liveTopic)
	watch := &pollWatch{}

	cfg := testConfig(t, brokers...)
	cfg.Logger = newWedgeLogger(t, wedgeTopic)
	cfg.ExtraOpts = []kgo.Opt{kgo.WithHooks(watch)}
	// Одна ячейка очереди: с умолчанием в шестнадцать батчей переполнить её
	// двумя записями нельзя, и dispatch до выбора между полной очередью и
	// мёртвым воркером просто не дошёл бы.
	cfg.Consumer.MessageQueueSize = 1

	sites := &consTrace{}
	cfg.OnPanic = func(_ context.Context, site PanicSite, _ any, _ []byte) {
		sites.add(string(site))
	}

	prod := consNewProducer(t, brokers)

	wedge := &mockHandler{}
	live := &mockHandler{}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, wedgeTopic, wedge)
	mustAddHandler(t, c, liveTopic, live)
	consStart(t, c)

	// Воркеры заводит колбэк назначения, поэтому воркер умирает ещё до первой
	// записи: к моменту отправки его done уже закрыт.
	waitFor(t, consWait, "воркер отравленного топика умер", func() bool {
		return slices.Contains(sites.snapshot(), string(PanicSitePartitionWorker))
	})

	prod.send(t, wedgeTopic, 0, "first")

	waitFor(t, consWait, "первая запись заняла очередь мёртвого воркера", func() bool {
		return watch.polled(wedgeTopic) >= 1
	})

	// Вторая запись обязана приехать отдельным опросом: в одном батче с первой
	// она заняла бы ту же ячейку очереди, и второй отправки — той самой, что
	// упирается в мёртвого воркера, — не случилось бы.
	prod.send(t, wedgeTopic, 0, "second")

	waitFor(t, consWait, "вторая запись вынута из буфера клиента", func() bool {
		return watch.polled(wedgeTopic) >= 2
	})

	prod.send(t, liveTopic, 0, "alive")

	waitFor(t, consWait, "живой топик обработан после отправки в мёртвого воркера", func() bool {
		return consHasValue(live.messages(), "alive")
	})

	if got := wedge.callCount(); got != 0 {
		t.Fatalf("обработчик мёртвой партиции вызван %d раз, want 0", got)
	}
}

// TestShutdownUnblocksDispatchStuckOnFullQueue — остановка разблокирует
// dispatch, упёршийся в полную очередь живого воркера.
//
// Класс дефекта тот же, что и у соседнего теста, но причина остановки другая:
// не мёртвый воркер, а backpressure. Очередь полна, воркер занят обработкой, и
// цикл опроса стоит в select — в этом состоянии его застаёт Stop.
//
// Честно про силу ассерта: саму ветку case <-ctx.Done() снаружи не увидеть.
// Сломайся она — dispatch всё равно разблокируется жёсткой отменой lifeCtx из
// awaitPollLoop, просто бюджетом позже. Поэтому тест фиксирует наблюдаемое:
// остановка доводится до конца, а не возвращает ErrPollLoopStuck, и ни одна
// запись не пропадает. Второе важнее: на отмене бросается и батч из dispatch, и
// хвост батча, уже полученного воркером, — обе потери были бы молчаливыми,
// потому что оффсеты нигде не отмечены и наружу ничего не сообщается.
func TestShutdownUnblocksDispatchStuckOnFullQueue(t *testing.T) {
	t.Parallel()

	const topic = "kafkax-backpressure-topic"

	brokers := newFakeCluster(t, 1, topic)
	watch := &pollWatch{}

	cfg := testConfig(t, brokers...)
	cfg.ExtraOpts = []kgo.Opt{kgo.WithHooks(watch)}
	// Три батча по две записи на одну ячейку очереди: первый достаётся воркеру,
	// второй занимает очередь, третий обязан остановить цикл опроса. Батч именно
	// парный, а не одиночный: пока воркер висит на первой записи, вторая ждёт
	// своей очереди уже внутри воркера, и жёсткая отмена обязана оборвать разбор
	// на ней — иначе Stop по истечении бюджета всё равно ждал бы обработки.
	cfg.Consumer.MaxPollRecords = 2
	cfg.Consumer.MessageQueueSize = 1
	// Воркер висит в обработчике до жёсткой отмены, и ровно GracefulTimeout
	// занимает его ожидание внутри Stop. Умолчание теста растянуло бы остановку
	// на пять секунд без всякой пользы для сценария.
	cfg.GracefulTimeout = 500 * time.Millisecond

	values := []string{"v0", "v1", "v2", "v3", "v4", "v5"}

	prod := consNewProducer(t, brokers)
	for _, v := range values {
		prod.send(t, topic, 0, v)
	}

	h := &blockOnCancel{}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	// Условие составное намеренно. Пока обработчик не вошёл, первый батч мог
	// лежать в очереди, а не у воркера, — тогда цикл опроса упёрся бы уже на
	// втором, третьего не случилось бы, и ожидание истекло бы впустую.
	waitFor(t, consWait, "цикл опроса упёрся в полную очередь воркера", func() bool {
		return h.entered.Load() >= 1 && watch.polled(topic) >= len(values)
	})

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v (ErrPollLoopStuck означал бы, что dispatch не увидел отмену)", err)
	}

	// Ровно один вход в обработчик: вторая запись батча лежала у воркера, и
	// жёсткая отмена обязана оборвать разбор перед ней. Пропади эта проверка из
	// цикла — обработчик получил бы v1 уже с отменённым контекстом, то есть
	// заведомо провальной попыткой, а Stop растянулся бы на весь хвост очереди.
	if got := h.entered.Load(); got != 1 {
		t.Fatalf("обработчик вызван %d раз, want 1: жёсткая отмена не оборвала разбор батча", got)
	}

	got := consDrainFresh(t, cfg, prod, topic, 0)

	want := append(slices.Clone(values), consMarkerValue)
	if !slices.Equal(got, want) {
		t.Fatalf("свежий консьюмер получил %v, want %v: отмена посреди backpressure потеряла записи",
			got, want)
	}
}

// TestMiddlewarePanicIsReportedAsHandlerPanic — паника middleware засчитывается
// как паника обработчика, а не обвязки.
//
// Различие не косметическое. Паника обвязки отравляет партицию немедленно и
// рапортуется как site=process_message; паника из цепочки middleware проходит
// политику повторов, повторяется HandlerMaxRetries+1 раз и рапортуется как
// site=handler. Причина — в AddHandler: Chain сворачивает цепочку в один
// обработчик ещё при регистрации, и вся она исполняется под recover'ом
// callHandler, где бы внутри неё паника ни случилась — до вызова next или после.
//
// Тест закрепляет это как контракт: аудит предполагал обратное (что middleware,
// упавшее до next, попадёт в PanicSiteProcessMessage), и ошибиться здесь легко —
// внешне «код обвязки» и «код цепочки» выглядят одинаково чужими.
//
//nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider: параллельный сосед смешал бы записи
func TestMiddlewarePanicIsReportedAsHandlerPanic(t *testing.T) {
	const topic = "kafkax-middleware-panic-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, topic)
	cfg := testConfig(t, brokers...)
	cfg.Consumer.HandlerMaxRetries = 1

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, consPoisonValue)

	mw := func(_ ConsumerHandler) ConsumerHandler {
		return ConsumerHandlerFunc(func(context.Context, IncomingMessage) error {
			panic("middleware exploded before next")
		})
	}

	h := &mockHandler{}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h, mw)
	consStart(t, c)

	consWaitTerminal(t, rec, topic, consumerStatusError, 1)

	if got := rec.sum(consMetricPanics, attribute.String("site", string(PanicSiteHandler))); got != 2 {
		t.Fatalf("panics(site=%s) = %d, want 2 (первый вызов и повтор)", PanicSiteHandler, got)
	}

	if got := rec.sum(consMetricPanics, attribute.String("site", string(PanicSiteProcessMessage))); got != 0 {
		t.Fatalf("panics(site=%s) = %d, want 0: цепочка middleware исполняется под recover'ом обработчика",
			PanicSiteProcessMessage, got)
	}

	if got := h.callCount(); got != 0 {
		t.Fatalf("обработчик вызван %d раз, want 0: middleware упало до вызова next", got)
	}
}

// wedgeLogHandler — slog.Handler, убивающий партиционного воркера одного
// конкретного топика.
//
// Отладочная запись о старте воркера — единственная точка runPartitionWorker,
// где вне processRecord исполняется чужой код, поэтому паника ставится именно
// на неё: на первой записи уровня Debug, у логгера которой в атрибутах стоит
// нужный топик. Атрибуты приходят из With() и потому лежат в самом хендлере, а
// не в записи; логгер franz-go строится от того же базового логгера, но топика
// в его цепочке With нет, так что перепутать поток franz-go с воркерным нельзя.
//
// Паника ровно одна: пересоздай кто-нибудь воркера, вторая закрутила бы тест в
// бесконечный цикл смертей вместо честного падения по таймауту.
type wedgeLogHandler struct {
	inner slog.Handler
	topic string
	fired *atomic.Bool
	attrs []slog.Attr
}

// newWedgeLogger собирает логгер, чья первая отладочная запись о воркере topic
// оборачивается паникой.
func newWedgeLogger(t *testing.T, topic string) *slog.Logger {
	t.Helper()

	return slog.New(&wedgeLogHandler{
		inner: testLogger(t).Handler(),
		topic: topic,
		fired: new(atomic.Bool),
	})
}

// Enabled пропускает всё: отбрось slog запись уровня Debug до Handle, паниковать
// было бы негде. Побочный эффект — включённый отладочный поток самого franz-go,
// который Handle гасит сам, не доводя до журнала теста.
func (h *wedgeLogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *wedgeLogHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Level == slog.LevelDebug && h.hasTopic() && h.fired.CompareAndSwap(false, true) {
		panic("kafkax test: slog handler blew up")
	}

	if !h.inner.Enabled(ctx, record.Level) {
		return nil
	}

	return h.inner.Handle(ctx, record)
}

func (h *wedgeLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	merged := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	merged = append(merged, h.attrs...)
	merged = append(merged, attrs...)

	return &wedgeLogHandler{
		inner: h.inner.WithAttrs(attrs),
		topic: h.topic,
		fired: h.fired,
		attrs: merged,
	}
}

func (h *wedgeLogHandler) WithGroup(name string) slog.Handler {
	return &wedgeLogHandler{
		inner: h.inner.WithGroup(name),
		topic: h.topic,
		fired: h.fired,
		attrs: h.attrs,
	}
}

func (h *wedgeLogHandler) hasTopic() bool {
	return slices.ContainsFunc(h.attrs, func(attr slog.Attr) bool {
		return attr.Key == "topic" && attr.Value.String() == h.topic
	})
}

// pollWatch считает записи, которые цикл опроса вынул из буфера клиента.
//
// Хук franz-go срабатывает внутри PollRecords, ДО передачи записи в dispatch:
// по нему тест узнаёт, что отправка воркеру вот-вот случится, и не гадает со
// sleep'ами, в каком именно опросе приехала запись. Это и делает сценарии с
// переполненной очередью воспроизводимыми.
type pollWatch struct {
	mu      sync.Mutex
	byTopic map[string]int
}

// Проверка на этапе компиляции: kgo.WithHooks принимает любое значение и молча
// игнорирует то, что ни одного интерфейса хука не реализует, — опечатка в
// сигнатуре стоила бы теста, зависшего на ожидании собственной оснастки.
var _ kgo.HookFetchRecordUnbuffered = (*pollWatch)(nil)

// OnFetchRecordUnbuffered реализует kgo.HookFetchRecordUnbuffered.
//
// polled=false означает, что запись выброшена сменой назначения, а не отдана
// опросу; для теста это не событие.
func (w *pollWatch) OnFetchRecordUnbuffered(record *kgo.Record, polled bool) {
	if !polled {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.byTopic == nil {
		w.byTopic = make(map[string]int)
	}

	w.byTopic[record.Topic]++
}

func (w *pollWatch) polled(topic string) int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.byTopic[topic]
}

// blockOnCancel — обработчик, который держит воркера занятым до отмены его
// контекста. Нужен сценариям про backpressure: только занятый воркер перестаёт
// разбирать очередь, оставляя цикл опроса упереться в неё.
type blockOnCancel struct {
	entered atomic.Int64
}

func (h *blockOnCancel) ProcessMessage(ctx context.Context, _ IncomingMessage) error {
	h.entered.Add(1)

	<-ctx.Done()

	return ctx.Err()
}
