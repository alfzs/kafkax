package kafkax

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// TestPanicSiteValuesAreStable стережёт строковые значения PanicSite.
//
// Тип и константы (RF-API-09) защищают потребителя от переименования, но не
// защищают дашборды: значение уходит в label site метрики kafkax.consumer.panics
// и в поле site лога, а на них построены алерты и таблица в README. Смена
// значения — ломающее изменение, невидимое компилятору, поэтому оно фиксируется
// здесь списком.
func TestPanicSiteValuesAreStable(t *testing.T) {
	t.Parallel()

	sites := []PanicSite{
		PanicSiteHandler,
		PanicSiteProcessMessage,
		PanicSitePartitionWorker,
		PanicSitePollLoop,
		PanicSiteMessageSkipped,
		PanicSitePanicHook,
	}

	// Склейкой, а не таблицей: так одно сравнение проверяет и сами значения, и
	// их различность — совпадение двух сайтов сделало бы метрику неразличимой
	// по site, ради чего атрибут и заведён.
	const want = "handler|process_message|partition_worker|poll_loop|on_message_skipped|on_panic"

	parts := make([]string, 0, len(sites))
	for _, site := range sites {
		parts = append(parts, string(site))
	}

	if got := strings.Join(parts, "|"); got != want {
		t.Errorf("значения PanicSite = %q, want %q\nновая точка recover? добавьте её в конец", got, want)
	}
}

// Логовая половина контракта «подавление обязано оставлять машиночитаемый след».
//
// Метрическая половина проверяется в наборе консьюмера: счётчик
// kafkax.consumer.panics с атрибутом site. Он отвечает только на вопрос
// «сколько», и по нему нельзя ни найти упавший код, ни отличить одну аварию от
// другой. Стек паники — единственное, по чему её вообще можно разобрать, и он
// живёт ровно в одной строке report; уберите её, и весь набор останется
// зелёным, а дежурный получит счётчик без единой подсказки, что именно упало.
//
// Тесты модульные, без брокера и консьюмера: panicReporter — самостоятельный
// объект, и гонять ради проверки формата записи целый кластер значило бы
// проверять формат через пять слоёв, которые к нему отношения не имеют.

// logRecord разбирает единственную запись JSON-журнала.
func logRecord(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()

	out := strings.TrimSpace(buf.String())
	if out == "" {
		t.Fatal("журнал пуст: подавленная паника не оставила следа")
	}

	lines := strings.Split(out, "\n")
	if len(lines) != 1 {
		t.Fatalf("записей в журнале = %d, want 1:\n%s", len(lines), buf.String())
	}

	rec := map[string]any{}
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("запись не разобралась как JSON: %v\n%s", err, lines[0])
	}

	return rec
}

// logString достаёт строковое поле записи; отсутствующее и нестроковое —
// пустая строка: для ассертов ниже это один и тот же исход «поля нет».
func logString(rec map[string]any, key string) string {
	s, ok := rec[key].(string)
	if !ok {
		return ""
	}

	return s
}

// wantLogField сверяет строковое поле записи.
func wantLogField(t *testing.T, rec map[string]any, key, want string) {
	t.Helper()

	if got := logString(rec, key); got != want {
		t.Errorf("поле %s = %v, want %q", key, rec[key], want)
	}
}

// TestReportLogsRecoveredPanicWithStack — report пишет панику в журнал целиком:
// сообщение, site, значение паники, стек и дополнительные атрибуты точки.
//
// Уровень проверяется наравне с полями: запись, уехавшая на Info, не попадёт ни
// в один алерт по уровню ошибок — то есть подавленная паника снова станет
// невидимой, хотя строка в коде осталась на месте.
func TestReportLogsRecoveredPanicWithStack(t *testing.T) {
	t.Parallel()

	// Стек снимает вызывающий, внутри своего defer: report получает его готовым
	// и обязан донести до журнала как есть.
	const stack = "goroutine 42 [running]:\nkafkax.(*partitionWorker).run(...)"

	var buf bytes.Buffer

	r := panicReporter{logger: slog.New(slog.NewJSONHandler(&buf, nil))}
	r.report(context.Background(), PanicSiteHandler, "handler exploded", []byte(stack),
		slog.String("topic", testTopic))

	rec := logRecord(t, &buf)

	wantLogField(t, rec, "msg", "Recovered panic")
	wantLogField(t, rec, "level", "ERROR")
	wantLogField(t, rec, "site", string(PanicSiteHandler))
	wantLogField(t, rec, "panic", "handler exploded")
	wantLogField(t, rec, "stack", stack)

	// extra — то, что превращает запись из «где-то упало» в «упало на этом
	// сообщении»: без топика, партиции и оффсета стек некуда приложить.
	wantLogField(t, rec, "topic", testTopic)
}

// TestPanicHookFailureIsLogged — паника внутри самого OnPanic попадает в журнал
// под своим site, не теряя чужого.
//
// Рекурсии в report здесь нет намеренно (повторный вызов того же хука кончился
// бы переполнением стека), поэтому лог в callHook — отдельная строка, и без
// ассерта она исчезла бы при первом же рефакторинге. Отказ обработчика паник —
// последнее, о чём приложение имеет право узнать молча: сигнальный компонент,
// отказавший без следа, неотличим от отсутствия аварий.
//
// Оба site обязаны быть в записи: on_panic отвечает «сломался хук», panic_site
// — «на чьей панике», и по одному только первому непонятно, что потеряно.
func TestPanicHookFailureIsLogged(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	r := panicReporter{
		logger: slog.New(slog.NewJSONHandler(&buf, nil)),
		onPanic: func(context.Context, PanicSite, any, []byte) {
			panic("hook exploded too")
		},
	}

	r.callHook(context.Background(), PanicSiteHandler, "handler exploded", nil)

	rec := logRecord(t, &buf)

	wantLogField(t, rec, "msg", "Recovered panic")
	wantLogField(t, rec, "level", "ERROR")
	wantLogField(t, rec, "site", string(PanicSitePanicHook))
	wantLogField(t, rec, "panic_site", string(PanicSiteHandler))
	wantLogField(t, rec, "panic", "hook exploded too")

	// Стек здесь снимает сам callHook: своего вызывающий не передавал, а без
	// стека запись говорит только «хук упал» и ни слова о том, где.
	if got := logString(rec, "stack"); !strings.Contains(got, "callHook") {
		t.Errorf("в стеке нет кадра callHook: %q", got)
	}
}
