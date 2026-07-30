package kafkax

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
)

// Запись в топике, для которого обработчик не зарегистрирован.
//
// В обычном пути такой записи не бывает: подписка строится по карте
// обработчиков (topics() → kgo.ConsumeTopics), и топик без обработчика просто не
// подписывается. Ветка достижима ровно одним честным способом — через
// Config.ExtraOpts, который по контракту добавляется к опциям клиента последним
// и перекрывает всё, что вывела библиотека. kgo.ConsumeTopics там пересоздаёт
// набор топиков целиком, и подписка расходится с картой обработчиков.
//
// Способ не надуманный: ExtraOpts описан как аварийный выход для настроек,
// которых пакет не покрывает, и подписка регулярным выражением
// (kgo.ConsumeRegex) — типовая причина им воспользоваться. Топик, подходящий под
// шаблон, но не заведённый в AddHandler, — то же самое состояние, только
// возникшее само.

// TestRecordWithoutHandlerPausesPartitionAtUncommittedOffset — запись без
// обработчика останавливает партицию, не сдвигая оффсет.
//
// Решение, которое фиксирует тест: рассинхрон подписки и карты обработчиков —
// это ошибка конфигурации, и реагировать на неё пакет обязан отравлением
// партиции, а не пропуском записи. Альтернатив ровно две, и обе хуже. Отметить
// оффсет значит потерять сообщение, которого никто не видел, из-за опечатки в
// ExtraOpts — притом бесшумно. Продолжать читать партицию, ничего не отмечая,
// значит крутить трафик в никуда: снаружи это неотличимо от «в топик просто не
// пишут». Отравление стоит громко и не теряет ничего.
//
// Цена решения — партиция стоит до тех пор, пока конфигурацию не починят.
// Восстановления внутри процесса нет и не предполагается: AddHandler после
// Start отвергается, потому что подписка уже уехала в kgo.ConsumeTopics и в
// общем случае поздняя регистрация дала бы обработчик без подписки, а
// консьюмер, прошедший Stop, не перезапускается. Чинится конфигурация, и
// поднимается новый экземпляр — он получит партицию назначением и снимет паузу
// вместе с созданием свежего воркера.
//
// Отсюда четыре ассерта. Гейдж пауз доказывает, что партиция действительно
// выведена из выборки, а не просто пропустила запись; свежий консьюмер в той же
// группе доказывает обратное — что запись не потеряна и вернётся новому
// владельцу. Порознь каждый из них проходил бы и в мире, где сообщение молча
// выброшено, и в мире, где партиция продолжает читаться поверх необработанной
// записи.
//
// Третий ассерт — на форму сигнала. Отказ обязан давать ровно одну запись
// уровня Error с машиночитаемым reason: раньше эта ветка писала отдельный Error
// перед poison, то есть один отказ приезжал дежурному двумя строками с разных
// уровней стека, и по журналу их было не свести.
//
// Четвёртый — на закрытость пути «дорегистрировать обработчик и поехать
// дальше». Он держит решение целиком: разрешив AddHandler после Start и не
// сняв при этом паузу, его сломают вместе с обещанием «пауза снимается только
// сменой воркера».
//
//nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider: параллельный сосед смешал бы записи
func TestRecordWithoutHandlerPausesPartitionAtUncommittedOffset(t *testing.T) {
	const (
		handledTopic   = "kafkax-handled-topic"
		unhandledTopic = "kafkax-unhandled-topic"
		orphanValue    = "orphan"
		wantReason     = "subscription and handler map are out of sync"
	)

	rec := captureMetrics(t)
	errs := &errorLog{}

	brokers := newFakeCluster(t, 1, handledTopic, unhandledTopic)
	cfg := testConfig(t, brokers...)
	cfg.Logger = slog.New(&errorLogHandler{inner: cfg.Logger.Handler(), log: errs})
	// Подписка шире карты обработчиков: ровно то состояние, в котором
	// processRecord не находит обработчика для приехавшей записи.
	cfg.ExtraOpts = []kgo.Opt{kgo.ConsumeTopics(handledTopic, unhandledTopic)}

	prod := consNewProducer(t, brokers)
	prod.send(t, unhandledTopic, 0, orphanValue)

	h := &mockHandler{}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, handledTopic, h)
	consStart(t, c)

	waitFor(t, consWait, "запись без обработчика получила исход error", func() bool {
		return rec.sum(consMetricProcessed,
			attribute.String("topic", unhandledTopic),
			attribute.String("status", consumerStatusError)) == 1
	})

	waitFor(t, consWait, "партиция без обработчика выведена из выборки", func() bool {
		return rec.sum(consMetricPaused) == 1
	})

	if got := h.callCount(); got != 0 {
		t.Fatalf("обработчик чужого топика вызван %d раз, want 0", got)
	}

	// Починить конфигурацию на лету нельзя: восстановление идёт только через
	// новый экземпляр консьюмера, и это часть принятого решения, а не
	// недосмотр.
	if err := c.AddHandler(unhandledTopic, &mockHandler{}); !errors.Is(err, ErrConsumerStarted) {
		t.Fatalf("AddHandler(%q) после Start = %v, want ErrConsumerStarted: "+
			"партиция снимается с паузы только сменой воркера, и поздняя регистрация "+
			"обработчика оставила бы её стоять с виду рабочей", unhandledTopic, err)
	}

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	entries := errs.snapshot()
	if len(entries) != 1 {
		t.Fatalf("записей уровня Error: %d, want 1 — один отказ, одна запись: %v", len(entries), entries)
	}

	if got := entries[0].attrs["reason"]; got != wantReason {
		t.Fatalf("reason = %q, want %q: причина отказа не машиночитаема", got, wantReason)
	}

	// Оффсет не отмечен: запись приедет следующему владельцу партиции.
	// Свежему консьюмеру расширенная подписка не нужна — обработчик у него
	// как раз для этого топика и есть, а лишний топик заставил бы его
	// отравиться на ровном месте.
	clean := cfg
	clean.Logger = testLogger(t)
	clean.ExtraOpts = nil

	got := consDrainFresh(t, clean, prod, unhandledTopic, 0)
	if len(got) != 2 || got[0] != orphanValue || got[1] != consMarkerValue {
		t.Fatalf("свежий консьюмер получил %v, want [%s %s]: оффсет отмечен за необработанной записью",
			got, orphanValue, consMarkerValue)
	}
}

// loggedError — запись уровня Error вместе с её атрибутами.
type loggedError struct {
	message string
	attrs   map[string]string
}

// errorLog — потокобезопасный журнал записей уровня Error.
//
// Соседний levelCount считает записи по уровням и о содержимом ничего не знает;
// здесь проверяется именно атрибут reason, ради которого отдельная строка Error
// из этой ветки и была убрана.
type errorLog struct {
	mu      sync.Mutex
	entries []loggedError
}

func (l *errorLog) add(record slog.Record) {
	attrs := make(map[string]string, record.NumAttrs())

	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.String()

		return true
	})

	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries = append(l.entries, loggedError{message: record.Message, attrs: attrs})
}

func (l *errorLog) snapshot() []loggedError {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]loggedError(nil), l.entries...)
}

// errorLogHandler складывает записи уровня Error в журнал, пропуская их дальше.
type errorLogHandler struct {
	inner slog.Handler
	log   *errorLog
}

func (h *errorLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *errorLogHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Level == slog.LevelError {
		h.log.add(record)
	}

	return h.inner.Handle(ctx, record)
}

func (h *errorLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &errorLogHandler{inner: h.inner.WithAttrs(attrs), log: h.log}
}

func (h *errorLogHandler) WithGroup(name string) slog.Handler {
	return &errorLogHandler{inner: h.inner.WithGroup(name), log: h.log}
}
