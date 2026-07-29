package kafkax

import (
	"context"
	"fmt"
	"sync"
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
// Отмена контекста — жёсткая остановка без дренажа, но Stop после неё обязан
// пройти до конца: он вызывается из defer'а, и зависание там означало бы
// процесс, который не завершается по SIGTERM.
func TestStartContextCancelStopsConsumer(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)

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
}

// TestGracefulStopDrainsInFlightMessage — сообщение, обрабатываемое в момент
// Stop, дообрабатывается и коммитится.
//
// Без этого graceful shutdown был бы фикцией: каждое развёртывание сервиса
// оставляло бы столько повторно обработанных сообщений, сколько воркеров было
// занято, и at-least-once начинал бы стоить заметных денег.
func TestGracefulStopDrainsInFlightMessage(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)
	cfg.Consumer.CommitInterval = time.Hour

	prod := consNewProducer(t, brokers)
	prod.send(t, testTopic, 0, "in-flight")

	entered := make(chan struct{})
	release := make(chan struct{})

	var enterOnce sync.Once

	h := &mockHandler{fn: func(int, IncomingMessage) error {
		enterOnce.Do(func() { close(entered) })
		<-release

		return nil
	}}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)
	consStart(t, c)

	select {
	case <-entered:
	case <-time.After(consWait):
		t.Fatal("обработчик так и не начал работу")
	}

	stopped := make(chan error, 1)
	go func() { stopped <- c.Stop() }()

	// Единственная возможная проверка того, что Stop НЕ бросил обработчика:
	// убедиться, что он не вернулся, пока обработка не завершена. Пауза
	// короткая — весь бюджет Stop здесь 5 секунд.
	select {
	case err := <-stopped:
		t.Fatalf("Stop вернулся, не дождавшись обработчика: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Stop = %v, want nil", err)
		}
	case <-time.After(consWait):
		t.Fatal("Stop не завершился после того, как обработчик вернул управление")
	}

	got := consDrainFresh(t, cfg, prod, testTopic, 0)
	if len(got) != 1 || got[0] != consMarkerValue {
		t.Fatalf("свежий консьюмер получил %v, want только маркер: дообработанное сообщение не закоммичено", got)
	}
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
	cfg.OnPanic = func(_ context.Context, site string, recovered any, stack []byte) {
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

	if steps := sites.snapshot(); len(steps) != 1 || steps[0] != panicSiteHandler+":handler exploded" {
		t.Fatalf("OnPanic вызван с %v, want [%s:handler exploded]", steps, panicSiteHandler)
	}

	if got := rec.sum(consMetricPanics, attribute.String("site", panicSiteHandler)); got != 1 {
		t.Fatalf("panics(site=%s) = %d, want 1", panicSiteHandler, got)
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
//nolint:paralleltest // подменяет глобальный MeterProvider
func TestOnPanicHookPanicDoesNotCrashConsumer(t *testing.T) {
	const topic = "kafkax-panic-hook-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 2, topic)
	cfg := testConfig(t, brokers...)
	cfg.OnPanic = func(context.Context, string, any, []byte) {
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
}
