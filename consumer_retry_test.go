package kafkax

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

// Тесты политики повторов и разрешения отказа — раздел «Политика повторов» в
// doc.go.
//
// Это самая нестандартная часть контракта пакета: отравленное сообщение по
// умолчанию не пропускается, а останавливает свою партицию. Тесты здесь
// проверяют именно то, что застрявшая партиция застревает, а не «как-нибудь
// проезжает»: молчаливый пропуск выглядит как нормальная работа и обнаруживается
// уже по недостающим данным.
//
// Почти каждый сценарий здесь считывает исход из метрик, а captureMetrics
// подменяет глобальный otel.MeterProvider — такие тесты обязаны идти
// последовательно, иначе записи соседей смешаются в одном журнале.

// errConsBoom — отказ обработчика в тестах.
var errConsBoom = errors.New("boom")

// consPoisonValue — значение «отравленного» сообщения: обработчик отказывает
// именно на нём, а тест затем ищет его же в том, что доехало.
const consPoisonValue = "poison"

// consProbeValue — контрольное сообщение в заведомо ЗДОРОВУЮ партицию.
//
// Им заменены паузы в доказательствах «больная партиция встала». Ожидание по
// часам доказывает такое утверждение ложно-отрицательно: тест зеленеет и тогда,
// когда пауза сломана, а запись из больной партиции просто не успела приехать.
// Контрольное сообщение отправляется уже после отказа и переводит проверку в
// позитивную: раз консьюмер успел сходить к брокеру за записью, которой в
// момент отказа ещё не существовало, то за записью больной партиции — лежащей
// на брокере с самого начала — он тем более успел бы сходить. Её отсутствие
// после этого означает именно паузу.
const consProbeValue = "healthy-probe"

// consWaitTerminal ждёт, пока сообщение получит окончательный исход в метрике.
//
// Ключевой приём для тестов повторов: как только счётчик processed вырос,
// цикл повторов гарантированно завершён, и число вызовов обработчика можно
// сравнивать точно, не гадая со sleep'ами.
func consWaitTerminal(t *testing.T, rec *recordedMetrics, topic, status string, want int64) {
	t.Helper()

	waitFor(t, consWait, "сообщение получило исход "+status, func() bool {
		return rec.sum(consMetricProcessed,
			attribute.String("topic", topic),
			attribute.String("status", status)) == want
	})
}

// TestHandlerNotRetriedByDefault — умолчание HandlerRetries: 0 означает один
// вызов, а не «повторять сколько-нибудь».
//
// Ноль как «без повторов» неочевиден: во многих клиентах он значит «повторять
// бесконечно». Ошибка в эту сторону превращает единичный отказ в бесконечно
// заблокированную партицию.
func TestHandlerNotRetriedByDefault(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider: параллельный сосед смешал бы записи
	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)

	prod := consNewProducer(t, brokers)
	prod.send(t, testTopic, 0, "v")

	h := &mockHandler{returnErr: errConsBoom}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)
	consStart(t, c)

	consWaitTerminal(t, rec, testTopic, consumerStatusError, 1)

	if got := h.callCount(); got != 1 {
		t.Fatalf("обработчик вызван %d раз, want 1 (HandlerRetries=0)", got)
	}

	if got := rec.sum(consMetricRetries, attribute.String("topic", testTopic)); got != 0 {
		t.Fatalf("счётчик повторов = %d, want 0", got)
	}
}

// TestHandlerRetriesExhausted — N повторов сверх первого вызова, всего N+1.
//
// Арифметика здесь ровно та, что описана в doc.go, и ошибиться в ней на единицу
// легко: attempt считает уже сделанные повторы, а не вызовы.
func TestHandlerRetriesExhausted(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider: параллельный сосед смешал бы записи
	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)
	cfg.Consumer.HandlerRetries = 2

	prod := consNewProducer(t, brokers)
	prod.send(t, testTopic, 0, "v")

	h := &mockHandler{returnErr: errConsBoom}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)
	consStart(t, c)

	consWaitTerminal(t, rec, testTopic, consumerStatusError, 1)

	if got := h.callCount(); got != 3 {
		t.Fatalf("обработчик вызван %d раз, want 3 (первый вызов + 2 повтора)", got)
	}

	// Счётчик повторов считает именно повторы, а не вызовы: по нему отличают
	// «обработчик иногда моргает» от «обработчик сдался с первого раза».
	if got := rec.sum(consMetricRetries, attribute.String("topic", testTopic)); got != 2 {
		t.Fatalf("счётчик повторов = %d, want 2", got)
	}
}

// TestHandlerSucceedsOnRetryCommitsOffset — сообщение, прошедшее с третьей
// попытки, считается обработанным, и его оффсет коммитится.
//
// Без коммита успешный после повторов ретрай был бы бесполезен: сообщение
// приезжало бы снова при каждом перезапуске, и обработчик падал бы на нём
// первые два раза вечно.
func TestHandlerSucceedsOnRetryCommitsOffset(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider: параллельный сосед смешал бы записи
	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)
	cfg.Consumer.HandlerRetries = 2

	prod := consNewProducer(t, brokers)
	prod.send(t, testTopic, 0, "flaky")

	h := &mockHandler{fn: func(call int, _ IncomingMessage) error {
		if call < 3 {
			return errConsBoom
		}

		return nil
	}}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)
	consStart(t, c)

	consWaitTerminal(t, rec, testTopic, consumerStatusSuccess, 1)

	if got := h.callCount(); got != 3 {
		t.Fatalf("обработчик вызван %d раз, want 3", got)
	}

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Оффсет закоммичен ⇔ свежий консьюмер в той же группе не получит ничего,
	// кроме дописанного маркера.
	got := consDrainFresh(t, cfg, prod, testTopic, 0)
	if len(got) != 1 || got[0] != consMarkerValue {
		t.Fatalf("свежий консьюмер получил %v, want только маркер: оффсет не закоммичен", got)
	}
}

// TestRetriesBlockOwnPartition — повторы блокируют свою партицию.
//
// Это цена порядка, а не недосмотр: следующее сообщение партиции не имеет права
// обогнать то, которое ещё повторяется, иначе отметка его оффсета сдвинула бы
// коммит за неудачное.
func TestRetriesBlockOwnPartition(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)
	// Бесконечные повторы: сообщение «a» держит партицию ровно столько, сколько
	// нужно тесту, и не уходит в разрешение отказа само по себе.
	cfg.Consumer.HandlerRetries = -1
	cfg.Consumer.HandlerRetryDelay = 10 * time.Millisecond

	prod := consNewProducer(t, brokers)
	prod.send(t, testTopic, 0, "a")
	prod.send(t, testTopic, 0, "b")

	release := make(chan struct{})

	h := &mockHandler{fn: func(_ int, msg IncomingMessage) error {
		if string(msg.Value) != "a" {
			return nil
		}

		select {
		case <-release:
			return nil
		default:
			return errConsBoom
		}
	}}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)
	consStart(t, c)

	waitFor(t, consWait, "первое сообщение ушло в повторы", func() bool {
		return h.callCount() >= 3
	})

	// Всё, что вызвано к этому моменту, обязано быть повторами «a»: появление
	// «b» означало бы, что порядок внутри партиции нарушен повторами.
	if consHasValue(h.messages(), "b") {
		t.Fatal("второе сообщение партиции обработано, пока первое повторяется: порядок нарушен")
	}

	close(release)

	waitFor(t, consWait, "партиция разблокирована и второе сообщение обработано", func() bool {
		return consHasValue(h.messages(), "b")
	})
}

// TestFailureWithoutSkipHookPausesPartition — центральная гарантия пакета.
//
// Без OnMessageSkipped отравленное сообщение не пропускается: его оффсет не
// отмечается, партиция ставится на паузу, а соседние продолжают работать.
// Если бы консьюмер поехал дальше по этой партиции, коммит следующего сообщения
// перепрыгнул бы неудачное, и оно исчезло бы бесследно — ровно та молчаливая
// потеря, ради предотвращения которой пакет переписан.
func TestFailureWithoutSkipHookPausesPartition(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider: параллельный сосед смешал бы записи
	const topic = "kafkax-poison-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 2, topic)
	cfg := testConfig(t, brokers...) // HandlerRetries=0, хука WithSkipHook нет

	// Значение reason — контракт наблюдаемости, а не украшение записи: по нему
	// отличают «хук не настроен» от «хук отказался», и алерт на первое строится
	// именно по нему. Литералом, потому что сверять его с той же строкой из
	// consumer_worker.go значило бы не проверять ничего: до этого ассерта обе
	// причины можно было переписать, не уронив ни одного теста.
	errs := &errorLog{}
	logger := WithLogger(slog.New(&errorLogHandler{inner: testLogger(t).Handler(), log: errs}))

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "p0-poison")
	prod.send(t, topic, 0, "p0-after")
	prod.send(t, topic, 1, "p1-first")

	h := &mockHandler{fn: func(_ int, msg IncomingMessage) error {
		if string(msg.Value) == "p0-poison" {
			return errConsBoom
		}

		return nil
	}}

	c := mustConsumer(t, cfg, logger)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	consWaitTerminal(t, rec, topic, consumerStatusError, 1)
	waitFor(t, consWait, "соседняя партиция обработана", func() bool {
		return consHasValue(h.messages(), "p1-first")
	})

	consRequireReason(t, errs, "no skip hook is configured")

	// Контрольное сообщение: соседняя партиция обязана продолжать принимать
	// новое — отравленное сообщение останавливает свою партицию, а не консьюмер
	// целиком, — и оно же служит опорой для проверки ниже (см. consProbeValue).
	prod.send(t, topic, 1, consProbeValue)

	waitFor(t, consWait, "здоровая партиция приняла контрольное сообщение", func() bool {
		return consHasValue(h.messages(), consProbeValue)
	})

	// «p0-after» лежит на брокере с начала теста, то есть был доступен раньше
	// контрольного сообщения. Раз то доехало, а это нет — партиция стоит.
	if consHasValue(h.messages(), "p0-after") {
		t.Fatal("сообщение за отравленным обработано: коммит уедет за непрокоммиченный оффсет")
	}

	if got := rec.sum(consMetricProcessed,
		attribute.String("topic", topic),
		attribute.String("status", consumerStatusSkipped)); got != 0 {
		t.Fatalf("processed(status=skipped) = %d, want 0: без хука сообщение не пропускается", got)
	}
}

// TestPoisonedMessageRedeliveredAfterRestart — отравленное сообщение приезжает
// снова.
//
// Пауза партиции имеет смысл только вместе с этим: сообщение не пропало, оно
// ждёт следующего запуска. Проверка замыкает гарантию at-least-once на
// наблюдаемое поведение, а не на внутренний флаг poisoned.
func TestPoisonedMessageRedeliveredAfterRestart(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider: параллельный сосед смешал бы записи
	const topic = "kafkax-redelivery-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, topic)
	cfg := testConfig(t, brokers...)

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, consPoisonValue)

	h := &mockHandler{returnErr: errConsBoom}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	consWaitTerminal(t, rec, topic, consumerStatusError, 1)

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	got := consDrainFresh(t, cfg, prod, topic, 0)

	want := []string{consPoisonValue, consMarkerValue}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("свежий консьюмер получил %v, want %v: отравленное сообщение потеряно", got, want)
	}
}

// TestSkipHookNilCommitsAndContinues — OnMessageSkipped, вернувший nil, забирает
// сообщение на себя.
//
// Это единственный штатный выход из отравленного сообщения: оффсет отмечается,
// партиция продолжает работу. Проверяются оба следствия сразу — иначе «партиция
// поехала дальше» могло бы означать «поехала, но с неотмеченным оффсетом», то
// есть скрытую потерю.
func TestSkipHookNilCommitsAndContinues(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider: параллельный сосед смешал бы записи
	const topic = "kafkax-skip-nil-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, topic)
	cfg := testConfig(t, brokers...)

	skipped := &consTrace{}
	skipHook := WithSkipHook(func(_ context.Context, msg IncomingMessage, _ error) error {
		skipped.add(string(msg.Value))

		return nil
	})

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, consPoisonValue)
	prod.send(t, topic, 0, "next")

	h := &mockHandler{fn: func(_ int, msg IncomingMessage) error {
		if string(msg.Value) == consPoisonValue {
			return errConsBoom
		}

		return nil
	}}

	c := mustConsumer(t, cfg, skipHook)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	consWaitTerminal(t, rec, topic, consumerStatusSkipped, 1)

	waitFor(t, consWait, "партиция продолжила работу", func() bool {
		return consHasValue(h.messages(), "next")
	})

	if steps := skipped.snapshot(); len(steps) != 1 || steps[0] != consPoisonValue {
		t.Fatalf("OnMessageSkipped получил %v, want [poison]", steps)
	}

	if got := rec.sum(consMetricProcessed,
		attribute.String("topic", topic),
		attribute.String("status", consumerStatusError)); got != 0 {
		t.Fatalf("processed(status=error) = %d, want 0: хук забрал сообщение", got)
	}

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	got := consDrainFresh(t, cfg, prod, topic, 0)
	if len(got) != 1 || got[0] != consMarkerValue {
		t.Fatalf("свежий консьюмер получил %v, want только маркер: пропущенное сообщение не закоммичено", got)
	}
}

// TestSkipHookErrorPausesPartition — хук, вернувший ошибку, ведёт себя как
// отсутствующий.
//
// Отказ хука — это «я не смог забрать сообщение», и трактовать его иначе
// значило бы терять данные ровно там, где потребитель пытался их сохранить
// (недоступный DLQ — типичный случай).
//
// Вторая партиция здесь нужна не сценарию, а доказательству: остановку
// партиции нельзя утверждать по тому, что за какое-то время ничего не приехало
// (см. consProbeValue).
func TestSkipHookErrorPausesPartition(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider: параллельный сосед смешал бы записи
	const topic = "kafkax-skip-err-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 2, topic)
	cfg := testConfig(t, brokers...)
	skipHook := WithSkipHook(func(context.Context, IncomingMessage, error) error {
		return errors.New("dlq unavailable")
	})

	// Второе значение той же пары; см. пояснение в
	// TestFailureWithoutSkipHookPausesPartition.
	errs := &errorLog{}
	logger := WithLogger(slog.New(&errorLogHandler{inner: testLogger(t).Handler(), log: errs}))

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, consPoisonValue)
	prod.send(t, topic, 0, "next")

	h := &mockHandler{fn: func(_ int, msg IncomingMessage) error {
		if string(msg.Value) == consPoisonValue {
			return errConsBoom
		}

		return nil
	}}

	c := mustConsumer(t, cfg, skipHook, logger)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	consWaitTerminal(t, rec, topic, consumerStatusError, 1)
	consRequireReason(t, errs, "the skip hook refused the message")

	// Контрольное сообщение в здоровую партицию: оно отправлено позже «next» и
	// доехало, значит у «next» время было.
	prod.send(t, topic, 1, consProbeValue)

	waitFor(t, consWait, "здоровая партиция приняла контрольное сообщение", func() bool {
		return consHasValue(h.messages(), consProbeValue)
	})

	if consHasValue(h.messages(), "next") {
		t.Fatal("партиция поехала дальше после отказа хука: оффсет отравленного сообщения будет перепрыгнут")
	}

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	got := consDrainFresh(t, cfg, prod, topic, 0)
	if len(got) == 0 || got[0] != consPoisonValue {
		t.Fatalf("свежий консьюмер получил %v, want сначала poison: сообщение потеряно", got)
	}
}

// TestSkipHookPanicPausesPartition — паника в хуке трактуется как его отказ.
//
// Хук исполняется в горутине воркера уже после того, как recover вокруг
// обработчика отработал: без собственного recover его паника уронила бы процесс.
// А «упал» обязано значить «не забрал»: иначе упавший хук молча разрешал бы
// сдвинуть коммит.
//
// Вторая партиция, как и в TestSkipHookErrorPausesPartition, — опора
// доказательства, а не часть сценария (см. consProbeValue).
func TestSkipHookPanicPausesPartition(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider: параллельный сосед смешал бы записи
	const topic = "kafkax-skip-panic-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 2, topic)
	cfg := testConfig(t, brokers...)
	skipHook := WithSkipHook(func(context.Context, IncomingMessage, error) error {
		panic("hook exploded")
	})

	sites := &consTrace{}
	panicHook := WithPanicHook(func(_ context.Context, site PanicSite, _ any, _ []byte) {
		sites.add(string(site))
	})

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, consPoisonValue)
	prod.send(t, topic, 0, "next")

	h := &mockHandler{fn: func(_ int, msg IncomingMessage) error {
		if string(msg.Value) == consPoisonValue {
			return errConsBoom
		}

		return nil
	}}

	c := mustConsumer(t, cfg, skipHook, panicHook)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	consWaitTerminal(t, rec, topic, consumerStatusError, 1)

	if steps := sites.snapshot(); len(steps) != 1 || steps[0] != string(PanicSiteMessageSkipped) {
		t.Fatalf("OnPanic вызван с %v, want [%s]", steps, PanicSiteMessageSkipped)
	}

	if got := rec.sum(consMetricPanics, attribute.String("site", string(PanicSiteMessageSkipped))); got != 1 {
		t.Fatalf("panics(site=%s) = %d, want 1", PanicSiteMessageSkipped, got)
	}

	// Партиция встала — паника хука не отличается по последствиям от его отказа.
	// Доказывается контрольным сообщением в здоровую партицию, а не выдержкой.
	prod.send(t, topic, 1, consProbeValue)

	waitFor(t, consWait, "здоровая партиция приняла контрольное сообщение", func() bool {
		return consHasValue(h.messages(), consProbeValue)
	})

	if consHasValue(h.messages(), "next") {
		t.Fatal("партиция поехала дальше после паники хука")
	}
}

// TestRetryCancelledDuringDelayDoesNotPoison — отмена контекста во время паузы
// между повторами.
//
// Вердикта обработчика в этот момент нет, поэтому сообщение не отмечается, но и
// партицию не травит: оно просто приедет снова. Считать здесь отказ значило бы
// ставить партицию на паузу при каждой штатной остановке процесса.
//
// Исход помечается отдельным cancelled, а не skipped: у skipped коммит уезжает
// за необработанную запись, здесь — нет. Тест проверяет обе стороны разделения,
// иначе дашборд «сколько сообщений мы потеряли» считал бы каждый деплой.
func TestRetryCancelledDuringDelayDoesNotPoison(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider: параллельный сосед смешал бы записи
	const topic = "kafkax-retry-cancel-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, topic)
	cfg := testConfig(t, brokers...)
	// Бесконечные повторы гарантируют, что после каждого отказа обработчика
	// управление уходит именно в паузу, а не в разрешение отказа: сценарий
	// становится детерминированным независимо от момента отмены.
	cfg.Consumer.HandlerRetries = -1
	cfg.Consumer.HandlerRetryDelay = 100 * time.Millisecond

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "stuck")

	h := &mockHandler{returnErr: errConsBoom}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)

	ctx, cancel := context.WithCancel(t.Context())
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitFor(t, consWait, "обработчик успел упасть хотя бы раз", func() bool {
		return h.callCount() >= 1
	})

	cancel()

	consWaitTerminal(t, rec, topic, consumerStatusCancelled, 1)

	if got := rec.sum(consMetricProcessed,
		attribute.String("topic", topic),
		attribute.String("status", consumerStatusError)); got != 0 {
		t.Fatalf("processed(status=error) = %d, want 0: отмена — не отказ обработчика", got)
	}

	if got := rec.sum(consMetricProcessed,
		attribute.String("topic", topic),
		attribute.String("status", consumerStatusSkipped)); got != 0 {
		t.Fatalf("processed(status=skipped) = %d, want 0: коммит за эту запись не сдвинулся", got)
	}

	// Длительность под cancelled не пишется: обработка не закончилась, мерить
	// нечего. Проверяется здесь, а не в тесте гистограммы, потому что это
	// единственный путь, порождающий cancelled.
	if got := rec.observations(consMetricDuration,
		attribute.String("topic", topic),
		attribute.String("status", consumerStatusCancelled)); len(got) != 0 {
		t.Fatalf("duration(status=cancelled) = %v, want none", got)
	}

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Не отмечено — значит приедет снова.
	got := consDrainFresh(t, cfg, prod, topic, 0)

	want := []string{"stuck", consMarkerValue}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("свежий консьюмер получил %v, want %v", got, want)
	}
}

// consRequireReason — среди записей уровня Error есть ровно одна с заданной
// причиной.
//
// Отдельным хелпером, потому что проверок таких две и обе про одно: значение
// reason различает ветки разрешения отказа, и по нему строят алерты. Ожидаемое
// приходит литералом из теста, а не из consumer_worker.go.
func consRequireReason(t *testing.T, errs *errorLog, want string) {
	t.Helper()

	var found int

	entries := errs.snapshot()
	for _, entry := range entries {
		if entry.attrs["reason"] == want {
			found++
		}
	}

	if found != 1 {
		t.Fatalf("записей с reason=%q: %d, want 1; все записи Error: %v", want, found, entries)
	}
}
