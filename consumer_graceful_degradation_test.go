package kafkax

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel/attribute"
)

// Тесты деградации graceful stop: что делает Stop, когда мягкая фаза в бюджет
// НЕ уложилась.
//
// Счастливые пути того же механизма проверены в consumer_lifecycle_test.go, и
// ими покрытие исчерпывалось: все существующие сценарии остановки укладываются
// в GracefulTimeout, поэтому путь «бюджет исчерпан, дорезаем принудительно» не
// исполнялся ни разу. Он же — самый дорогой: именно на нём консьюмер бросает
// обработчик, оставляет оффсет незакоммиченным и, в худшем случае, сознательно
// не закрывает клиента Kafka.
//
// Оба сценария меряют длительность относительно бюджета, поэтому параллельными
// быть не могут.

// consStopAsync возвращает результат Stop, вызванного из отдельной горутины.
//
// Прямой вызов здесь не годится: оба сценария держат внутри консьюмера что-то
// незавершённое, и Stop, который всё-таки решит этого дождаться, обязан валить
// тест по своему потолку, а не вешать весь прогон до общего таймаута go test.
func consStopAsync(t *testing.T, c *KafkaConsumer) error {
	t.Helper()

	stopped := make(chan error, 1)

	go func() { stopped <- c.Stop() }()

	select {
	case err := <-stopped:
		return err
	case <-time.After(consWait):
		t.Fatal("Stop не вернулся сам: завершение удерживается тем, что должно было быть брошено")

		return nil
	}
}

// TestStopAbandonsHandlerIgnoringCancellation — обработчик, не реагирующий на
// отмену контекста, не удерживает Stop дольше его бюджета.
//
// Класс дефекта: бесконечный graceful shutdown. Дренаж очередей полезен ровно
// до тех пор, пока он ограничен: обработчик, который не смотрит на ctx (чужая
// библиотека, сетевой вызов без дедлайна, обычный select без ctx.Done), иначе
// держал бы Stop столько, сколько ему угодно, — то есть процесс не завершался
// бы по SIGTERM и его добивал бы SIGKILL уже без всякого дренажа.
//
// Ассерты устроены так, чтобы отличить «Stop уложился в бюджет» от «Stop успел,
// потому что обработчик успел». Поэтому проверяется не только длительность, но
// и то, что обработчик на момент возврата Stop ВСЁ ЕЩЁ держит сообщение: без
// этого тест проходил бы и на реализации, которая честно дождалась воркера.
//
// Оффсет проверяется по внешнему эффекту (consDrainFresh), а не по внутреннему
// флагу: брошенное сообщение обязано приехать снова, иначе принудительное
// дорезание означало бы молчаливую потерю данных.
//
//nolint:paralleltest // измеряет длительность Stop относительно бюджета и читает метрики через глобальный MeterProvider
func TestStopAbandonsHandlerIgnoringCancellation(t *testing.T) {
	const topic = "kafkax-stop-abandon-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, topic)
	cfg := testConfig(t, brokers...)
	// Мягкая фаза заведомо короче времени, которое обработчик держит сообщение.
	cfg.GracefulTimeout = 200 * time.Millisecond
	// По этому же бюджету идут жёсткая добивка воркеров и финальный коммит. С
	// умолчанием в 5 секунд Stop дождался бы зависшего воркера целиком, и ветка
	// «не дождались даже после отмены» снова осталась бы непройденной.
	cfg.Consumer.RebalanceTimeout = 300 * time.Millisecond
	// Тикер автокоммита не должен сработать: иначе оффсет сдвинулся бы сам, и
	// проверка повторной доставки перестала бы что-либо доказывать.
	cfg.Consumer.CommitInterval = time.Hour

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "stuck")

	entered := make(chan struct{})
	release := make(chan struct{})

	var (
		enterOnce sync.Once
		returned  atomic.Bool
	)

	// Обработчик намеренно не смотрит на ctx — это и есть воспроизводимый
	// проблемный случай. Верхняя граница в 2 секунды нужна не сценарию, а
	// гигиене: упавший по другому ассерту тест не должен оставлять после себя
	// вечно заблокированную горутину.
	h := &mockHandler{fn: func(int, IncomingMessage) error {
		enterOnce.Do(func() { close(entered) })

		select {
		case <-release:
		case <-time.After(2 * time.Second):
		}

		returned.Store(true)

		return nil
	}}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	select {
	case <-entered:
	case <-time.After(consWait):
		t.Fatal("обработчик так и не начал работу")
	}

	start := time.Now()
	stopErr := consStopAsync(t, c)
	elapsed := time.Since(start)

	// Обработчик всё ещё внутри: значит, Stop его бросил, а не дождался.
	// Единственный ассерт, отличающий деградацию от везения планировщика.
	if returned.Load() {
		t.Fatal("обработчик успел вернуться до конца Stop: сценарий не проверил принудительное дорезание")
	}

	// Бюджет — GracefulTimeout + RebalanceTimeout на добивку воркеров +
	// RebalanceTimeout на финальный коммит, то есть 800 мс. Потолок взят с
	// многократным запасом: он ловит «Stop дождался обработчика» (2 секунды), а
	// не миллисекунды планировщика.
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("Stop занял %s при GracefulTimeout=%s и RebalanceTimeout=%s",
			elapsed, cfg.GracefulTimeout, cfg.Consumer.RebalanceTimeout)
	}

	// Коммитить нечего: обработчик вердикта не дал, MarkCommitRecords не
	// вызывался. Ошибка здесь означала бы, что Stop сообщает о провале коммита
	// там, где коммита не было.
	if stopErr != nil {
		t.Fatalf("Stop = %v, want nil", stopErr)
	}

	// Гейдж воркеров считает живые горутины, а не желаемое состояние: Stop уже
	// вернулся, а воркер ещё работает — и метрика обязана это показывать. Ноль
	// здесь означал бы, что декремент делается «по намерению остановить», и
	// брошенный воркер стал бы невидим ровно в том сценарии, ради которого
	// метрика и заведена.
	if got := rec.sum(consMetricWorkers); got != 1 {
		t.Fatalf("workers.active = %d сразу после Stop, want 1: брошенный воркер не виден в метрике", got)
	}

	// Исчерпанный бюджет дренажа — это оборванная обработка и, скорее всего,
	// дубликаты после перезапуска. Событие одно на бюджет, а не на воркера:
	// бюджет общий, и «не уложились» — одно решение по всем оставшимся сразу.
	// Сигнал прикладной: он означает, что GracefulTimeout мал для реального
	// времени обработки, и без счётчика это видно только тому, кто читает логи
	// каждого деплоя.
	if got := rec.sum(consMetricDrainTimeouts,
		attribute.String("phase", phaseWorkers)); got != 1 {
		t.Fatalf("drain.timeouts{phase=%s} = %d, want 1: исчерпание бюджета дренажа "+
			"видно только в логе", phaseWorkers, got)
	}

	close(release)

	// Воркер обязан всё-таки выйти сам: отмена контекста его не убивала, но и
	// вечно жить он не должен.
	waitFor(t, consWait, "брошенный воркер завершился", func() bool {
		return rec.sum(consMetricWorkers) == 0
	})

	// Сообщение не закоммичено — оно приедет снова. Ровно ради этого оффсет и
	// не двигают: брошенная обработка не считается успешной.
	got := consDrainFresh(t, cfg, prod, topic, 0)

	if want := []string{"stuck", consMarkerValue}; !slices.Equal(got, want) {
		t.Fatalf("свежий консьюмер получил %v, want %v: брошенное сообщение потеряно", got, want)
	}
}

// consLogGate — точка, в которой логирование намертво встаёт.
//
// Общее состояние вынесено в отдельную структуру, потому что slog копирует
// обработчик на каждом With: логгер консьюмера собирается через
// Logger.With("group", ...), и обёртка обязана переживать WithAttrs, сохраняя
// те же каналы.
type consLogGate struct {
	// message — точный текст записи, на которой обработчик блокируется. Именно
	// текст, а не уровень: через тот же логгер пишет и franz-go, а встать нужно
	// ровно в горутине цикла опроса.
	message string
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

// consBlockingLogHandler — slog.Handler, зависающий на записи consLogGate.message.
//
// Это воспроизведение штатной причины, названной в документации ErrPollLoopStuck:
// цикл опроса висит в чужом коде, который отмену контекста не проверяет.
// Заблокировать его изнутри пакета нечем — все внутренние ожидания на ctx
// смотрят.
type consBlockingLogHandler struct {
	inner slog.Handler
	gate  *consLogGate
}

// Enabled отвечает true безусловно, не спрашивая вложенный обработчик: slog
// зовёт Handle только для разрешённой записи, а вложенный здесь — выбрасывающий,
// и его собственный ответ (всегда false) закрыл бы ворота вместе с логами.
func (consBlockingLogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h consBlockingLogHandler) Handle(ctx context.Context, rec slog.Record) error {
	if rec.Message == h.gate.message {
		h.gate.once.Do(func() { close(h.gate.entered) })
		<-h.gate.release
	}

	return h.inner.Handle(ctx, rec)
}

func (h consBlockingLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return consBlockingLogHandler{inner: h.inner.WithAttrs(attrs), gate: h.gate}
}

func (h consBlockingLogHandler) WithGroup(name string) slog.Handler {
	return consBlockingLogHandler{inner: h.inner.WithGroup(name), gate: h.gate}
}

// TestStopReportsStuckPollLoopAndLeavesClientOpen — цикл опроса, застрявший в
// чужом коде, не даёт закрыть клиента, и Stop сообщает об этом ErrPollLoopStuck.
//
// Класс дефекта: падение процесса на штатном завершении. CloseAllowingRebalance
// при живом цикле опроса снимает гейт BlockRebalanceOnPoll, удерживаемый чужой
// горутиной, и запускает onPartitionsRevoked параллельно с dispatch — гонку за
// картой воркеров и «send on closed channel» в горутине без вызывающего
// (01-concurrency.md, К3). Пакет выбирает утечку одного клиента вместо этого,
// поэтому проверяются оба следствия сразу: и возвращённый сентинел, и то, что
// клиент остался открыт. Без второго ассерта тест проходил бы и на реализации,
// которая сообщает об ошибке, но всё равно закрывает клиента, — то есть ровно на
// той, из-за которой находка и появилась.
//
// Зависание сделано снаружи, через slog.Handler: изнутри пакета цикл опроса
// заблокировать нечем, все его собственные ожидания смотрят на отмену. Запись
// «Consumer group session error» пишется прямо из цикла опроса, а отказ группы
// вызывается фатальной ошибкой на JoinGroup — это детерминированно и не требует
// ни гонок, ни пауз.
//
//nolint:paralleltest // измеряет длительность Stop относительно бюджета
func TestStopReportsStuckPollLoopAndLeavesClientOpen(t *testing.T) {
	const topic = "kafkax-poll-stuck-topic"

	cluster, brokers := newFakeClusterHandle(t, 1, topic)

	gate := &consLogGate{
		message: "Consumer group session error",
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}

	cfg := testConfig(t, brokers...)
	// Логи уходят в никуда намеренно: клиент этого консьюмера останется открытым
	// по условию сценария, его горутины переживут тест, а запись в t.Output()
	// после завершения теста роняет прогон.
	cfg.Logger = slog.New(consBlockingLogHandler{
		inner: slog.DiscardHandler,
		gate:  gate,
	})
	// Мягкий и жёсткий бюджеты ожидания цикла опроса: Stop обязан вернуться
	// примерно за их сумму, а не висеть вместе с циклом.
	cfg.GracefulTimeout = 200 * time.Millisecond
	cfg.Consumer.RebalanceTimeout = 300 * time.Millisecond

	// Фатальный (неретраибельный) код на JoinGroup: franz-go признаёт сессию
	// группы проигранной и подкидывает ErrGroupSession синтетическим фетчем —
	// тем самым, разбор которого и логирует цикл опроса.
	cluster.ControlKey(kmsg.JoinGroup.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		resp, ok := req.ResponseKind().(*kmsg.JoinGroupResponse)
		if !ok {
			return nil, nil, false
		}

		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code

		return resp, nil, true
	})

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, &mockHandler{})
	consStart(t, c)

	select {
	case <-gate.entered:
	case <-time.After(consWait):
		t.Fatal("цикл опроса так и не дошёл до разбора отказа группы")
	}

	// Освобождение — в Cleanup, а не в конце теста: до конца может не дойти, а
	// заблокированный цикл опроса удерживает горутину и клиента.
	t.Cleanup(func() {
		close(gate.release)

		// Цикл опроса выходит сам, как только логирование отпустит его. Ждать
		// обязательно: клиента закрывает следующая строка, а закрывать его при
		// живом цикле — та самая гонка.
		select {
		case <-c.loopDone:
		case <-time.After(consWait):
			t.Error("цикл опроса не вышел даже после освобождения логгера")

			return
		}

		// Stop сознательно не закрыл клиента, поэтому это делает тест: иначе
		// утечка переехала бы в следующие тесты прогона.
		c.client.CloseAllowingRebalance()
	})

	start := time.Now()
	stopErr := consStopAsync(t, c)
	elapsed := time.Since(start)

	if !errors.Is(stopErr, ErrPollLoopStuck) {
		t.Fatalf("Stop = %v, ожидался ErrPollLoopStuck", stopErr)
	}

	// Бюджет — GracefulTimeout на мягкое ожидание плюс RebalanceTimeout на
	// жёсткое, то есть 500 мс. Потолок с многократным запасом: он ловит «Stop
	// дождался цикла опроса», а не миллисекунды планировщика.
	if elapsed > 3*time.Second {
		t.Fatalf("Stop занял %s при GracefulTimeout=%s и RebalanceTimeout=%s",
			elapsed, cfg.GracefulTimeout, cfg.Consumer.RebalanceTimeout)
	}

	// Клиент обязан остаться открытым — см. документацию ErrPollLoopStuck.
	// Проверяется через поле, потому что наблюдаемого признака у этого решения
	// нет по построению: закрытый клиент отвечал бы ErrClientClosed на любой
	// запрос, а открытый — ходит к брокеру, который в этом тесте жив.
	if err := c.client.Ping(t.Context()); errors.Is(err, kgo.ErrClientClosed) {
		t.Fatal("клиент закрыт при живом цикле опроса: ровно та гонка, ради которой существует ErrPollLoopStuck")
	}
}
