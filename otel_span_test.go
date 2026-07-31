package kafkax

import (
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
)

// Что консьюмер пишет в спан обработки — и сколько раз.
//
// Класс дефекта здесь один и он самый неприятный из наблюдаемых: трейс,
// приходящий зелёным при неуспешной обработке. Функциональные тесты его не
// ловят вовсе — сообщение отработано «как положено», партиция отравлена,
// метрики посчитаны, — а человек, разбирающий инцидент по трассировке, видит
// исправный span и уходит искать причину в другое место. Симметричный дефект —
// шум: спан, помеченный ошибкой на каждой промежуточной попытке, превращает
// одно неудачное сообщение в набор разных аварий в UI трейсинга.
//
// Отсюда форма всех ассертов в файле: считается ЧИСЛО записей об ошибке во всех
// выданных спанах, а не факт «ошибка где-то есть». Контракт свежий и держится
// на одной строке в runHandler: спан трогается только на исчерпании повторов, а
// callHandler не трогает его намеренно, чтобы паника на последней попытке не
// приезжала в трассировку дважды. Ассерт «>= 1» этот контракт не удержал бы.
//
// Провайдер трейсера глобальный и читается в конструкторе консьюмера, поэтому
// тесты здесь непараллельные и создают консьюмер уже после подмены.

// spanCaptureTracer подменяет глобальный TracerProvider записывающим на время
// теста и возвращает трейсер.
func spanCaptureTracer(t *testing.T) *recordingTracer {
	t.Helper()

	tracer := &recordingTracer{}
	prev := otel.GetTracerProvider()

	otel.SetTracerProvider(recordingTracerProvider{tracer: tracer})
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	return tracer
}

// spanOnlyErr требует, чтобы во всей трассировке была ровно одна запись об
// ошибке, и отдаёт её.
//
// Отдельный хелпер, потому что «ровно одна» — общая часть контракта для всех
// сценариев отказа: и обычной ошибки, и паники. Число попыток в сообщении есть
// не для красоты: «записей 3 при 3 вызовах обработчика» читается сразу как
// «отметку вернули на путь повтора».
func spanOnlyErr(t *testing.T, tracer *recordingTracer, calls int) error {
	t.Helper()

	errs := tracer.recordedErrs()
	if len(errs) != 1 {
		t.Fatalf("записей RecordError во всех спанах: %d, want 1 (вызовов обработчика: %d)", len(errs), calls)
	}

	return errs[0]
}

// spanOnlyErrored требует ровно один спан со статусом codes.Error и отдаёт его
// описание.
//
// Статус проверяется отдельно от RecordError: это разные вызовы, и потерять
// можно любой из них. Спан с записанной ошибкой, но статусом Unset выглядит в
// UI зелёным ровно так же, как спан без ошибки вовсе, — то есть дефект,
// который RecordError-ассерт пропустил бы целиком.
func spanOnlyErrored(t *testing.T, tracer *recordingTracer) string {
	t.Helper()

	errored := tracer.erroredSpans()
	if len(errored) != 1 {
		t.Fatalf("спанов со статусом codes.Error: %d, want 1", len(errored))
	}

	_, desc := errored[0].status()

	return desc
}

// TestProcessSpanRecordsHandlerFailureOnce — исчерпание повторов оставляет в
// трассировке ровно одну отметку об ошибке.
//
// Сценарий подобран так, чтобы отличать «отметку ставят на отказ» от «отметку
// ставят на каждую неудачную попытку»: обработчик зовётся трижды (два повтора)
// и падает каждый раз, а запись об ошибке обязана быть одна. При ассерте на
// «хотя бы одну» тест прошёл бы в обоих мирах, и разница между ними — одно
// сообщение против трёх аварий в UI трейсинга.
//
// Хук OnMessageSkipped намеренно не задан: без него resolveFailure травит
// партицию, воркер встаёт, и повторной доставки того же сообщения — а с ней и
// второго честного RecordError — быть не может. Иначе счёт зависел бы от того,
// сколько кругов сделал брокер до Stop.
//
//nolint:paralleltest // подменяет глобальный TracerProvider
func TestProcessSpanRecordsHandlerFailureOnce(t *testing.T) {
	const (
		topic     = "kafkax-span-failure-topic"
		wantCalls = 3
	)

	tracer := spanCaptureTracer(t)

	brokers := newFakeCluster(t, 1, topic)
	cfg := testConfig(t, brokers...)
	cfg.Consumer.HandlerRetries = 2

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "boom")

	h := &mockHandler{returnErr: errConsBoom}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	waitFor(t, consWait, "обработчик исчерпал попытки", func() bool {
		return h.callCount() >= wantCalls
	})
	waitFor(t, consWait, "отказ доехал до спана", func() bool {
		return len(tracer.recordedErrs()) >= 1
	})

	// Остановка до подсчёта: пока воркеры живы, число записей — движущаяся
	// величина, и «ровно одна» проверялась бы на случайном срезе времени.
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	got := spanOnlyErr(t, tracer, h.callCount())
	if !errors.Is(got, errConsBoom) {
		t.Errorf("в спан записана %v, ожидалась ошибка обработчика %v", got, errConsBoom)
	}

	// Описание статуса — то, что видно в списке спанов без разворачивания
	// событий, поэтому пустая строка здесь равносильна потере причины отказа.
	if desc := spanOnlyErrored(t, tracer); desc != errConsBoom.Error() {
		t.Errorf("описание статуса = %q, want %q", desc, errConsBoom.Error())
	}
}

// TestProcessSpanRecordsHandlerPanicOnce — паника обработчика приезжает в спан
// один раз, а не отдельно от callHandler и отдельно от runHandler.
//
// Отдельный сценарий от обычной ошибки, потому что у паники два кандидата на
// запись: callHandler, который её ловит и рапортует полным пакетом сигналов
// (стек, счётчик, OnPanic), и runHandler, который получает её уже как обычную
// ошибку. Стоит вернуть RecordError в первый — и паника на последней попытке
// удвоится в трассировке, читаясь как две разные аварии в одном сообщении.
// Полный рапорт при этом остаётся односторонним обязательством callHandler:
// тест проверяет ровно границу между ними, а не наличие рапорта.
//
// Ошибка обязана быть распознаваемой как ErrHandlerPanic: спан с текстом
// «panic: ...» без сентинела не даёт отличить аварию кода обработчика от
// штатного отказа, а это разные инциденты.
//
//nolint:paralleltest // подменяет глобальный TracerProvider
func TestProcessSpanRecordsHandlerPanicOnce(t *testing.T) {
	const (
		topic     = "kafkax-span-panic-topic"
		wantCalls = 2
	)

	tracer := spanCaptureTracer(t)

	brokers := newFakeCluster(t, 1, topic)
	cfg := testConfig(t, brokers...)
	cfg.Consumer.HandlerRetries = 1

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "boom")

	h := &mockHandler{fn: func(int, IncomingMessage) error {
		panic("handler exploded")
	}}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	waitFor(t, consWait, "обработчик запаниковал на всех попытках", func() bool {
		return h.callCount() >= wantCalls
	})
	waitFor(t, consWait, "паника доехала до спана", func() bool {
		return len(tracer.recordedErrs()) >= 1
	})

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	got := spanOnlyErr(t, tracer, h.callCount())
	if !errors.Is(got, ErrHandlerPanic) {
		t.Errorf("в спан записана %v, ожидалась обёртка над ErrHandlerPanic", got)
	}

	if desc := spanOnlyErrored(t, tracer); !strings.Contains(desc, "handler exploded") {
		t.Errorf("описание статуса = %q, в нём нет текста паники", desc)
	}
}

// TestProcessSpanStaysCleanOnSuccess — успешная обработка не оставляет в спане
// ни ошибки, ни статуса Error.
//
// Обратная половина того же контракта. Тест на отказ ловит недостачу записей,
// но не ловит их избыток на исправном пути: реализация, помечающая ошибкой
// каждый спан подряд, прошла бы его целиком. А ложный Error стоит дороже
// пропущенного — он обесценивает фильтр по статусу, которым инцидент и ищут.
//
// Проверяется весь трейсер, а не спан обработки: отметка, поставленная не на
// том спане, для человека в UI неотличима от отметки на нужном.
//
//nolint:paralleltest // подменяет глобальный TracerProvider
func TestProcessSpanStaysCleanOnSuccess(t *testing.T) {
	const topic = "kafkax-span-success-topic"

	tracer := spanCaptureTracer(t)

	brokers := newFakeCluster(t, 1, topic)
	cfg := testConfig(t, brokers...)

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "payload")

	h := &mockHandler{}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	waitFor(t, consWait, "сообщение обработано", func() bool {
		return h.callCount() >= 1
	})

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Спаны обязаны быть: без них тест ничего не проверял бы — пустая
	// трассировка тривиально свободна от ошибок.
	if len(tracer.started()) == 0 {
		t.Fatal("не заведено ни одного спана — kotel не подключён к консьюмеру")
	}

	if errs := tracer.recordedErrs(); len(errs) != 0 {
		t.Errorf("на успешной обработке записано %d ошибок: %v", len(errs), errs)
	}

	for _, span := range tracer.started() {
		if code, desc := span.status(); code == codes.Error {
			t.Errorf("спан помечен codes.Error на успешной обработке: %q", desc)
		}
	}
}
