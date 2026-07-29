package kafkax

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Точки восстановления паник — значение атрибута site у метрики
// kafkax.consumer.panics.
//
// Множество замкнутое и низкокардинальное (по одному значению на defer с
// recover), поэтому пригодно как label метрики. Значения совпадают с именами
// функций, в которых стоит recover, — по значению site видно, что именно
// упало, без чтения стека из лога.
//
// Все значения консьюмерские: у продюсера собственных горутин нет, поэтому
// перехватывать в нём нечего.
const (
	panicSiteHandler          = "handler"
	panicSiteProcessMessage   = "process_message"
	panicSitePartitionWorker  = "partition_worker"
	panicSitePollLoop         = "poll_loop"
	panicSiteMessageSkipped   = "on_message_skipped"
	panicSitePanicHookHandler = "on_panic"
)

// panicReporter — реакция на восстановленную панику: лог, метрика и
// пользовательский хук.
//
// Поле есть только у консьюмера, и это не упущение: собственных горутин у
// продюсера нет, восстанавливать панику негде и не из чего — она уходит
// вызывающему по его же стеку. Config.OnPanic поэтому тоже вызывается только
// консьюмером.
//
// Не пробрасывать панику наружу — правильное решение: rethrow из воркерной
// горутины уронил бы процесс, поскольку паника чужой горутины вызывающим кодом
// не ловится. Но подавление обязано оставлять машиночитаемый след, иначе с
// точки зрения дашбордов паника в обработчике не отличается от отсутствия
// трафика.
type panicReporter struct {
	logger  *slog.Logger
	panics  metric.Int64Counter
	onPanic func(ctx context.Context, site string, recovered any, stack []byte)
}

// report фиксирует восстановленную панику в site.
//
// stack снимает вызывающий (внутри самого defer, пока кадр паники ещё жив), а
// не report: debug.Stack() отсюда показал бы стек report, а не место падения.
//
// extra — дополнительные атрибуты лога, специфичные для точки: топик,
// партиция, оффсет. В метрику они не идут: там только site.
func (r panicReporter) report(ctx context.Context, site string, recovered any, stack []byte, extra ...slog.Attr) {
	args := make([]any, 0, len(extra)+3)
	args = append(args,
		slog.String("site", site),
		slog.Any("panic", recovered),
		slog.String("stack", string(stack)))

	for _, attr := range extra {
		args = append(args, attr)
	}

	r.logger.Error("Recovered panic", args...)

	// Проверка на nil, а не безусловный Add: report вызывается из defer'ов,
	// которые обязаны отработать и на частично собранном объекте — в том числе
	// в тестах, конструирующих консьюмер литералом.
	if r.panics != nil {
		r.panics.Add(ctx, 1, metric.WithAttributes(attribute.String("site", site)))
	}

	r.callHook(ctx, site, recovered, stack)
}

// callHook вызывает OnPanic под собственным recover.
//
// Хук — чужой код, исполняемый в горутине библиотеки уже после того, как
// внешний recover отработал: его собственная паника прошла бы мимо и уронила
// процесс — ровно та авария, от которой хук должен был предупреждать.
func (r panicReporter) callHook(ctx context.Context, site string, recovered any, stack []byte) {
	if r.onPanic == nil {
		return
	}

	defer func() {
		if hookPanic := recover(); hookPanic != nil {
			// Намеренно без рекурсии в report: паника внутри обработчика паник
			// не должна ни инкрементировать метрику чужого site, ни повторно
			// звать тот же хук.
			r.logger.Error("Recovered panic",
				slog.String("site", panicSitePanicHookHandler),
				slog.String("panic_site", site),
				slog.Any("panic", hookPanic))
		}
	}()

	r.onPanic(ctx, site, recovered, stack)
}
