package kafkax

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

// Тесты жизненного цикла консьюмера: Start/Stop, ребаланс, паники.
//
// Общая тема — что происходит с уже принятыми, но ещё не закоммиченными
// сообщениями, когда консьюмер останавливают или у него отбирают партиции.
// Именно на этих переходах теряются данные, и именно они хуже всего покрываются
// ручной проверкой.

// TestStopCommitsMarkedOffsets — Stop коммитит отмеченное и идемпотентен.
//
// Полагаться на тикер автокоммита нельзя: между последней успешной обработкой и
// остановкой процесса может не пройти ни одного интервала, и всё обработанное
// приехало бы заново. Идемпотентность нужна не меньше: Stop зовут и из
// defer'а, и из обработчика сигнала.
func TestStopCommitsMarkedOffsets(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)
	// Интервал автокоммита заведомо больше времени теста: единственный шанс
	// закоммитить оффсет — финальный коммит внутри Stop.
	cfg.Consumer.CommitInterval = time.Hour

	prod := consNewProducer(t, brokers)
	prod.send(t, testTopic, 0, "done")

	h := &mockHandler{}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)
	consStart(t, c)

	waitFor(t, consWait, "сообщение обработано", func() bool { return h.callCount() == 1 })

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if err := c.Stop(); err != nil {
		t.Fatalf("повторный Stop = %v, want nil (идемпотентность)", err)
	}

	got := consDrainFresh(t, cfg, prod, testTopic, 0)
	if len(got) != 1 || got[0] != consMarkerValue {
		t.Fatalf("свежий консьюмер получил %v, want только маркер: Stop не закоммитил оффсет", got)
	}
}

// TestStopReportsFailedFinalCommit — провал финального коммита виден в ошибке
// Stop, а не только в логе.
//
// Сценарий не экзотический: брокер уезжает ровно в тот момент, когда под ним
// перезапускают сервис. Обработанное осталось незакоммиченным и после старта
// приедет заново — для at-least-once это штатно, но потребитель обязан узнать
// про дубликаты из возвращённого значения. Без сентинела этот исход неотличим
// от чистой остановки, потому что оба возвращаются из одного Stop.
//
// Кластер гасится целиком: подделать отказ именно коммита подменой
// конфигурации нельзя, а обрыв связи с координатором — та самая причина, по
// которой ветка вообще существует.
//
// Вторая половина того же контракта — метрика. Дисциплины «читать возврат
// Stop» у типового `defer c.Stop()` нет, и без счётчика проваленный финальный
// коммит — самый частый источник дубликатов после деплоя — не существовал бы
// ни для одного алерта. Записи в лог при этом быть не должно: ошибка уходит
// вызывающему, он её и залогирует, а пакет, залогировав сам, удваивал бы
// событие в журнале.
//
//nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider
func TestStopReportsFailedFinalCommit(t *testing.T) {
	rec := captureMetrics(t)

	cluster, brokers := newFakeClusterHandle(t, 1, testTopic)

	levels := &levelCount{}

	cfg := testConfig(t, brokers...)
	cfg.Logger = slog.New(&levelCountHandler{inner: cfg.Logger.Handler(), count: levels})
	// Логи franz-go выключены целиком: мёртвый кластер он комментирует своими
	// записями уровня Error («group manage loop errored»), и без порога тест
	// считал бы их наравне с записями пакета. Заодно это единственный сценарий,
	// где KafkaLogLevel="none" проверяется в деле.
	cfg.KafkaLogLevel = KafkaLogNone
	// Автокоммит не должен успеть: иначе к моменту Stop коммитить будет нечего,
	// franz-go вернёт nil, не сходив к брокеру, и тест проверял бы пустоту.
	cfg.Consumer.CommitInterval = time.Hour
	// Бюджет финального коммита — RebalanceTimeout. На мёртвом брокере он
	// выбирается целиком, поэтому берётся близкий к минимуму.
	cfg.Consumer.RebalanceTimeout = 500 * time.Millisecond

	prod := consNewProducer(t, brokers)
	prod.send(t, testTopic, 0, "processed")

	h := &mockHandler{}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)
	consStart(t, c)

	waitFor(t, consWait, "сообщение обработано", func() bool { return h.callCount() == 1 })

	// Оффсет отмечен, но не закоммичен — коммитить будет что, а некуда.
	cluster.Close()

	err := c.Stop()
	if !errors.Is(err, ErrCommitFailed) {
		t.Fatalf("Stop = %v, ожидался ErrCommitFailed", err)
	}

	// Причина сохранена рядом с сентинелом: без неё дежурный видит «commit
	// failed» и не знает, координатор ли недоступен, истёк ли бюджет или
	// отобрали партиции. Разворачивается списком, потому что обёрнуты обе
	// ошибки сразу.
	list := cfgUnwrapJoined(t, err)
	if len(list) != 2 {
		t.Fatalf("развернулось %v, ожидались сентинел и причина", list)
	}

	if errors.Is(list[1], ErrCommitFailed) {
		t.Fatalf("вместо причины развернулся тот же сентинел: %v", list[1])
	}

	if got := rec.sum(consMetricCommitErrors,
		attribute.String("phase", phaseShutdown)); got != 1 {
		t.Fatalf("commit.errors{phase=%s} = %d, want 1: проваленный финальный коммит "+
			"не существует ни для одного алерта", phaseShutdown, got)
	}

	// Ни одной записи Error от самого пакета: событие уже уехало вызывающему
	// возвратом. Логи franz-go отключены выше, так что считать больше нечего.
	if got := levels.of(slog.LevelError); got != 0 {
		t.Fatalf("записей уровня Error: %d, want 0 — ошибка возвращается, а не логируется", got)
	}
}

// TestStopWithoutStart — Stop до Start не падает и не виснет.
//
// Обычный путь при неудачной инициализации приложения: часть компонентов
// создана, запуск не дошёл, defer Stop всё равно отработает.
func TestStopWithoutStart(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)

	c := mustConsumer(t, cfg)
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop без Start = %v, want nil", err)
	}
}

// TestStartContextCancelStopsConsumer — отмена контекста Start останавливает
// консьюмера, и последующий Stop не виснет.
//
// Отмена контекста запускает тот же путь, что и явный Stop. Явный Stop после
// неё обязан пройти до конца и вернуть результат того же завершения: он
// вызывается из defer'а, и зависание там означало бы процесс, который не
// завершается по SIGTERM.
func TestStartContextCancelStopsConsumer(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)
	// Как и в TestStopCommitsMarkedOffsets: тикер автокоммита заведомо не
	// сработает, поэтому закоммиченный оффсет доказывает, что отмена контекста
	// прошла полный путь завершения, а не оборвала консьюмера на месте.
	cfg.Consumer.CommitInterval = time.Hour

	prod := consNewProducer(t, brokers)
	prod.send(t, testTopic, 0, "v")

	h := &mockHandler{}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)

	ctx, cancel := context.WithCancel(t.Context())
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitFor(t, consWait, "сообщение обработано", func() bool { return h.callCount() == 1 })

	cancel()

	done := make(chan error, 1)
	go func() { done <- c.Stop() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stop после отмены контекста = %v, want nil", err)
		}
	case <-time.After(consWait):
		t.Fatal("Stop завис после отмены контекста Start")
	}

	got := consDrainFresh(t, cfg, prod, testTopic, 0)
	if len(got) != 1 || got[0] != consMarkerValue {
		t.Fatalf("свежий консьюмер получил %v, want только маркер: отмена контекста "+
			"не довела завершение до финального коммита", got)
	}
}

// Шаги дренажа in-flight сообщения, из порядка которых собирается
// доказательство в TestGracefulStopDrainsInFlightMessage.
const (
	drainStepHandlerEntered  = "handler-entered"
	drainStepStopEntered     = "stop-entered"
	drainStepHandlerReturned = "handler-returned"
	drainStepStopReturned    = "stop-returned"
)

// TestGracefulStopDrainsInFlightMessage — сообщение, обрабатываемое в момент
// Stop, дообрабатывается и коммитится.
//
// Без этого graceful shutdown был бы фикцией: каждое развёртывание сервиса
// оставляло бы столько повторно обработанных сообщений, сколько воркеров было
// занято, и at-least-once начинал бы стоить заметных денег.
//
// «Stop не бросил обработчика» — утверждение о порядке, а не о длительности, и
// снимается оно порядком меток в общем журнале. Прежняя проверка «за 200 мс
// Stop не вернулся» отказывала ложно-отрицательно: на загруженной машине
// брошенный обработчик успел бы не уложиться в это окно, и тест позеленел бы.
// Метка stop-entered снимается с терминального состояния консьюмера, которое
// взводится первой же строкой завершения: с этого момента любой возврат Stop до
// метки handler-returned означает брошенного обработчика — в каком бы порядке
// метки затем ни легли.
func TestGracefulStopDrainsInFlightMessage(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)
	cfg.Consumer.CommitInterval = time.Hour

	prod := consNewProducer(t, brokers)
	prod.send(t, testTopic, 0, "in-flight")

	steps := &consTrace{}
	release := make(chan struct{})

	h := &mockHandler{fn: func(int, IncomingMessage) error {
		steps.add(drainStepHandlerEntered)
		<-release
		steps.add(drainStepHandlerReturned)

		return nil
	}}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)
	consStart(t, c)

	waitFor(t, consWait, "обработчик начал работу", func() bool {
		return consHasStep(steps, drainStepHandlerEntered)
	})

	stopped := make(chan error, 1)

	go func() {
		err := c.Stop()

		steps.add(drainStepStopReturned)

		stopped <- err
	}()

	// Завершение началось — обработчик при этом всё ещё стоит на release.
	waitFor(t, consWait, "Stop вошёл в завершение", func() bool {
		return c.loadState() == consumerClosed
	})
	steps.add(drainStepStopEntered)

	close(release)

	waitFor(t, consWait, "Stop завершился после возврата обработчика", func() bool {
		return len(steps.snapshot()) == 4
	})

	if err := <-stopped; err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}

	want := []string{
		drainStepHandlerEntered,
		drainStepStopEntered,
		drainStepHandlerReturned,
		drainStepStopReturned,
	}
	if got := steps.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("порядок шагов %v, want %v: Stop не дождался обработчика", got, want)
	}

	got := consDrainFresh(t, cfg, prod, testTopic, 0)
	if len(got) != 1 || got[0] != consMarkerValue {
		t.Fatalf("свежий консьюмер получил %v, want только маркер: дообработанное сообщение не закоммичено", got)
	}
}

// consHasStep сообщает, попал ли шаг в журнал.
func consHasStep(steps *consTrace, step string) bool {
	return slices.Contains(steps.snapshot(), step)
}

// TestConsumerRebalanceSharesPartitions — два консьюмера одной группы делят
// партиции, а после ухода одного второй забирает всё.
//
// Ребаланс — единственный момент, когда партиция меняет владельца вместе с
// незакоммиченными оффсетами. Проверяется не распределение само по себе (его
// выбирает балансировщик), а то, что ни одно отправленное сообщение не пропало
// ни при разделении партиций, ни при их возврате.
//
//nolint:paralleltest // держится на секундной сессии группы: под параллельной нагрузкой брокер начнёт выкидывать консьюмеров
func TestConsumerRebalanceSharesPartitions(t *testing.T) {
	const topic = "kafkax-rebalance-topic"

	brokers := newFakeCluster(t, 2, topic)
	cfg := testConfig(t, brokers...)
	// Умолчания (сессия 6s, heartbeat 1s) растянули бы ребаланс на секунды:
	// проверяется логика колбэков, а не выдержка таймаутов группы.
	cfg.Consumer.SessionTimeout = time.Second
	cfg.Consumer.HeartbeatInterval = 200 * time.Millisecond
	cfg.Consumer.RebalanceTimeout = 2 * time.Second

	prod := consNewProducer(t, brokers)

	hA := &mockHandler{}
	first := mustConsumer(t, cfg)
	mustAddHandler(t, first, topic, hA)
	consStart(t, first)

	hB := &mockHandler{}
	second := mustConsumer(t, cfg)
	mustAddHandler(t, second, topic, hB)
	consStart(t, second)

	// Пока идёт ребаланс, партиции могут принадлежать кому угодно, поэтому
	// сообщения подкладываются в обе до тех пор, пока каждый консьюмер не
	// получит хотя бы одно. Ждать «завершения ребаланса» напрямую нечем:
	// наблюдаемый признак раздела партиций — это и есть работа обоих.
	var produced []string

	deadline := time.Now().Add(consWait)
	for i := 0; hA.callCount() == 0 || hB.callCount() == 0; i++ {
		if time.Now().After(deadline) {
			t.Fatalf("партиции не поделены: A получил %d сообщений, B — %d", hA.callCount(), hB.callCount())
		}

		value := fmt.Sprintf("m%02d", i)
		prod.send(t, topic, int32(i%2), value)
		produced = append(produced, value)

		// Пауза здесь — ограничение темпа отправки, а не ожидание результата.
		time.Sleep(20 * time.Millisecond)
	}

	// Ни одно сообщение не пропало: дубликаты при ребалансе штатны, потери — нет.
	waitFor(t, consWait, "все отправленные сообщения доехали до кого-то из двоих", func() bool {
		return consHasAll(append(hA.messages(), hB.messages()...), produced)
	})

	if err := second.Stop(); err != nil {
		t.Fatalf("Stop второго консьюмера: %v", err)
	}

	// Оставшийся обязан забрать освободившиеся партиции: иначе уход одного
	// экземпляра при выкатке остановил бы половину трафика навсегда.
	var after []string

	for i := range 4 {
		value := fmt.Sprintf("after%02d", i)
		prod.send(t, topic, int32(i%2), value)
		after = append(after, value)
	}

	waitFor(t, consWait, "оставшийся консьюмер забрал все партиции", func() bool {
		return consHasAll(hA.messages(), after)
	})

	if err := first.Stop(); err != nil {
		t.Fatalf("Stop первого консьюмера: %v", err)
	}
}

// TestHandlerPanicRecovered — паника обработчика не убивает воркера.
//
// Неперехваченная паника унесла бы горутину партиции вместе с её очередью, и
// коммит следующего воркера перепрыгнул бы всё, что в ней осталось. Здесь
// паника превращается в обычный отказ: сообщение идёт по политике повторов,
// партиция встаёт на паузу, консьюмер продолжает работать.
//
//nolint:paralleltest // подменяет глобальный MeterProvider
func TestHandlerPanicRecovered(t *testing.T) {
	const topic = "kafkax-panic-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 2, topic)
	cfg := testConfig(t, brokers...)

	sites := &consTrace{}
	cfg.OnPanic = func(_ context.Context, site PanicSite, recovered any, stack []byte) {
		if len(stack) == 0 {
			sites.add("empty-stack")

			return
		}

		sites.add(fmt.Sprintf("%s:%v", site, recovered))
	}

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "boom")
	prod.send(t, topic, 1, "ok")

	h := &mockHandler{fn: func(_ int, msg IncomingMessage) error {
		if string(msg.Value) == "boom" {
			panic("handler exploded")
		}

		return nil
	}}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	consWaitTerminal(t, rec, topic, consumerStatusError, 1)

	if steps := sites.snapshot(); len(steps) != 1 || steps[0] != string(PanicSiteHandler)+":handler exploded" {
		t.Fatalf("OnPanic вызван с %v, want [%s:handler exploded]", steps, PanicSiteHandler)
	}

	if got := rec.sum(consMetricPanics, attribute.String("site", string(PanicSiteHandler))); got != 1 {
		t.Fatalf("panics(site=%s) = %d, want 1", PanicSiteHandler, got)
	}

	// Консьюмер жив: соседняя партиция обрабатывается и до, и после паники.
	waitFor(t, consWait, "соседняя партиция обработана", func() bool {
		return consHasValue(h.messages(), "ok")
	})

	prod.send(t, topic, 1, "ok-after-panic")

	waitFor(t, consWait, "консьюмер продолжает работать после паники", func() bool {
		return consHasValue(h.messages(), "ok-after-panic")
	})

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Паника — это отказ, а не успех: оффсет не отмечен, сообщение приедет снова.
	got := consDrainFresh(t, cfg, prod, topic, 0)
	if len(got) == 0 || got[0] != "boom" {
		t.Fatalf("свежий консьюмер получил %v, want сначала boom: паника засчитана как успех", got)
	}
}

// TestOnPanicHookPanicDoesNotCrashConsumer — паника внутри самого OnPanic.
//
// Хук вызывается уже после того, как внешний recover отработал, поэтому его
// собственная паника прошла бы мимо и уронила процесс — ровно та авария, о
// которой хук должен был предупредить.
//
// Пережить её мало: подавление обязано оставлять машиночитаемый след. Рекурсии
// в report здесь нет намеренно (повторный вызов того же хука кончился бы
// переполнением стека), поэтому счётчик инкрементится отдельной строкой в
// callHook — и без ассерта ниже эта строка тихо исчезла бы при первом же
// рефакторинге, вернув отказ мониторинга паник в исходное молчаливое состояние.
//
//nolint:paralleltest // подменяет глобальный MeterProvider
func TestOnPanicHookPanicDoesNotCrashConsumer(t *testing.T) {
	const topic = "kafkax-panic-hook-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 2, topic)
	cfg := testConfig(t, brokers...)
	cfg.OnPanic = func(context.Context, PanicSite, any, []byte) {
		panic("hook exploded too")
	}

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "boom")
	prod.send(t, topic, 1, "ok")

	h := &mockHandler{fn: func(_ int, msg IncomingMessage) error {
		if string(msg.Value) == "boom" {
			panic("handler exploded")
		}

		return nil
	}}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	consWaitTerminal(t, rec, topic, consumerStatusError, 1)

	waitFor(t, consWait, "консьюмер пережил панику в OnPanic", func() bool {
		return consHasValue(h.messages(), "ok")
	})

	prod.send(t, topic, 1, "ok-after")

	waitFor(t, consWait, "здоровая партиция продолжает работать", func() bool {
		return consHasValue(h.messages(), "ok-after")
	})

	// Паника хука посчитана под своим site. Ровно одна: рапорт о панике
	// обработчика пишется только на первой попытке, значит и хук зван один раз.
	if got := rec.sum(consMetricPanics, attribute.String("site", string(PanicSitePanicHook))); got != 1 {
		t.Fatalf("panics(site=%s) = %d, want 1: отказ обработчика паник не виден в метриках",
			PanicSitePanicHook, got)
	}

	// Site чужой паники подменяться не должен: по нему строится алерт на
	// «упал обработчик», и смешать его с «упал хук» значило бы потерять оба.
	if got := rec.sum(consMetricPanics, attribute.String("site", string(PanicSiteHandler))); got != 1 {
		t.Fatalf("panics(site=%s) = %d, want 1", PanicSiteHandler, got)
	}
}

// TestStopDrainsWorkersBeforeFinalCommit — оффсет сообщения, дообработанного на
// остановке, коммитит сам Stop, а не колбэк отзыва внутри закрытия клиента.
//
// Фаза дренажа в Stop выглядит дублированием, и в наблюдаемом поведении почти
// им и является: CloseAllowingRebalance выводит участника из группы, уход
// вызывает onPartitionsRevoked, а тот делает и тот же дренаж, и тот же
// CommitMarkedOffsets. Разница не в том, что происходит, а в том, кому
// достаётся отказ. Коммит из колбэка отзыва вернуть некому — колбэк зовёт
// franz-go, а не приложение, — поэтому его провал существует только как строка
// в логе и счётчик с phase=revoke.
//
// Убери фазу дренажа, и к финальному коммиту in-flight сообщение ещё не
// отмечено: коммитить нечего, franz-go отвечает nil, не сходив к брокеру, Stop
// возвращает «всё хорошо», а отмеченный уже после этого оффсет уезжает в
// коммит отзыва — и там теряется молча. Дежурный узнаёт о потере по дубликатам
// после следующего деплоя, и никак иначе.
//
// Обработчик отпускается не по таймеру, а по исчезновению своего воркера с
// карты консьюмера: снятие с карты — первое, что делает фаза дренажа, и оно
// строго предшествует финальному коммиту. С вырезанной фазой карту чистит уже
// onPartitionsRevoked, то есть после коммита, так что тест краснеет
// детерминированно, а не по стечению таймингов.
//
//nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider
func TestStopDrainsWorkersBeforeFinalCommit(t *testing.T) {
	const topic = "kafkax-stop-drain-commit-topic"

	rec := captureMetrics(t)
	cluster, brokers := newFakeClusterHandle(t, 1, topic)
	failOffsetCommits(cluster)

	cfg := testConfig(t, brokers...)
	// Логи franz-go выключены: отвергнутый коммит он комментирует своими
	// записями Error, а к предмету теста они отношения не имеют.
	cfg.KafkaLogLevel = KafkaLogNone
	// Автокоммит не должен успеть: иначе оффсет уедет тикером до Stop, и
	// финальному коммиту снова будет нечего отправлять.
	cfg.Consumer.CommitInterval = time.Hour
	// NOT_COORDINATOR ретраибелен, и franz-go повторяет коммит до конца
	// бюджета. Бюджет — RebalanceTimeout, поэтому он взят близким к минимуму:
	// иначе тест простаивал бы в повторах дважды, на обоих коммитах.
	cfg.Consumer.RebalanceTimeout = 500 * time.Millisecond

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "in-flight")

	key := workerKey{topic: topic, partition: 0}
	entered := make(chan struct{})

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, ConsumerHandlerFunc(func(ctx context.Context, _ IncomingMessage) error {
		close(entered)
		consAwaitWorkerDropped(ctx, c, key)

		return nil
	}))
	consStart(t, c)

	select {
	case <-entered:
	case <-time.After(consWait):
		t.Fatal("обработчик не получил сообщение")
	}

	err := c.Stop()
	if !errors.Is(err, ErrCommitFailed) {
		t.Fatalf("Stop = %v, want ErrCommitFailed: оффсет дообработанного сообщения "+
			"коммитится не финальным коммитом, и его провал вызывающему не виден", err)
	}

	if got := rec.sum(consMetricCommitErrors, attribute.String("phase", phaseShutdown)); got != 1 {
		t.Fatalf("commit.errors{phase=%s} = %d, want 1: отказ повешен не на ту фазу",
			phaseShutdown, got)
	}
}

// consAwaitWorkerDropped ждёт, пока воркер партиции исчезнет с карты консьюмера.
//
// Отмена контекста прекращает ожидание: без неё обработчик, которого никто не
// снял с карты, держал бы дренаж до жёсткой отмены и превращал бы падение теста
// в таймаут всего пакета.
func consAwaitWorkerDropped(ctx context.Context, c *Consumer, key workerKey) {
	for {
		c.workersMu.Lock()
		_, alive := c.workers[key]
		c.workersMu.Unlock()

		if !alive {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// TestStopDrainBudgetIsGracefulTimeout — мягкая фаза Stop укладывается в
// GracefulTimeout, а не в бюджет ребаланса.
//
// GracefulTimeout — это то, что оркестратор знает как
// terminationGracePeriodSeconds: превысив его, процесс получает SIGKILL, и
// финального коммита не будет вовсе — то есть graceful shutdown обернётся ровно
// той повторной обработкой, ради предотвращения которой он и написан.
//
// Дренаж через onPartitionsRevoked ограничен RebalanceTimeout и про
// GracefulTimeout не знает. Умолчания пакета (3m против 1m) разницу прячут, но
// её получает всякий, кто уменьшил бюджет завершения и не тронул таймаут
// ребаланса, — а это ровно то, что делают, подгоняя завершение под
// terminationGracePeriod. Поэтому фазу дренажа держит Stop: только у неё бюджет
// тот, который обещан.
//
// Ассерт на верхнюю границу, а не на равенство: он ложно краснеет лишь на
// машине, которая на порядок медленнее, зато вырезанная фаза даёт восемь секунд
// против полутысячи миллисекунд и мимо него не проходит.
func TestStopDrainBudgetIsGracefulTimeout(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)
	cfg.GracefulTimeout = 500 * time.Millisecond
	cfg.Consumer.RebalanceTimeout = 8 * time.Second
	cfg.Consumer.CommitInterval = time.Hour

	prod := consNewProducer(t, brokers)
	prod.send(t, testTopic, 0, "stuck")

	entered := make(chan struct{})

	c := mustConsumer(t, cfg)
	// Обработчик отпускает партицию только по отмене — то есть ровно по
	// жёсткой добивке, которой мягкая фаза заканчивается, исчерпав бюджет.
	mustAddHandler(t, c, testTopic, ConsumerHandlerFunc(func(ctx context.Context, _ IncomingMessage) error {
		close(entered)
		<-ctx.Done()

		return nil
	}))
	consStart(t, c)

	select {
	case <-entered:
	case <-time.After(consWait):
		t.Fatal("обработчик не получил сообщение")
	}

	start := time.Now()

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}

	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Stop занял %s при GracefulTimeout=%s и RebalanceTimeout=%s: "+
			"дренаж ушёл в бюджет ребаланса, а не в бюджет завершения",
			elapsed, cfg.GracefulTimeout, cfg.Consumer.RebalanceTimeout)
	}
}
