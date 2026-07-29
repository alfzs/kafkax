package kafkax

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Тесты потери партиций — колбэка onPartitionsLost.
//
// Потеря отличается от отзыва тем, что сессия группы к моменту колбэка уже
// мертва: партиции не «передаются», а отбираются задним числом. Отсюда и
// асимметрия реализации — отзыв коммитит отмеченное, потеря намеренно не
// коммитит. Асимметрия не видна ни в сигнатурах, ни в состоянии пакета, и
// «выравнивание» одного колбэка по другому — правка на одну строку, которая не
// ломает ничего, кроме гарантии at-least-once.

// Тексты записей, по которым тест отличает потерю от отзыва. Колбэки ребаланса
// ничего не возвращают и не меняют наблюдаемого извне состояния, поэтому лог —
// единственный признак того, какой из них отработал.
const (
	logPartitionsLost    = "Partitions lost"
	logPartitionsRevoked = "Partitions revoked"
)

// logMessages — потокобезопасный журнал текстов записей: пишет в него
// slog.Handler из горутин franz-go, читает тест из своей.
type logMessages struct {
	mu   sync.Mutex
	msgs []string
}

func (l *logMessages) add(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.msgs = append(l.msgs, msg)
}

func (l *logMessages) count(msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := 0

	for _, m := range l.msgs {
		if m == msg {
			n++
		}
	}

	return n
}

// logSpy — slog.Handler, запоминающий тексты записей и передающий их дальше.
//
// WithAttrs и WithGroup обязаны сохранять журнал: библиотека логирует и через
// производные логгеры, и обёртка, теряющая себя на первом With, пропустила бы
// часть записей.
type logSpy struct {
	inner slog.Handler
	seen  *logMessages
}

func (h logSpy) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h logSpy) Handle(ctx context.Context, rec slog.Record) error {
	h.seen.add(rec.Message)

	return h.inner.Handle(ctx, rec)
}

func (h logSpy) WithAttrs(attrs []slog.Attr) slog.Handler {
	return logSpy{inner: h.inner.WithAttrs(attrs), seen: h.seen}
}

func (h logSpy) WithGroup(name string) slog.Handler {
	return logSpy{inner: h.inner.WithGroup(name), seen: h.seen}
}

// spyLogger возвращает логгер теста и журнал того, что через него прошло.
func spyLogger(t *testing.T) (*slog.Logger, *logMessages) {
	t.Helper()

	seen := &logMessages{}

	return slog.New(logSpy{inner: testLogger(t).Handler(), seen: seen}), seen
}

// fenceHeartbeats заставляет kfake отвечать на Heartbeat кодом code и отдаёт
// функцию, снимающую подмену.
//
// Подменяется ровно ответ, а не состояние группы внутри kfake: участник для
// брокера остаётся живым, его оффсеты по-прежнему принимаются. Это и нужно
// тесту потери — иначе «оффсет не сдвинулся» ничего не доказывало бы, потому
// что коммит отверг бы сам брокер, а не библиотека решила бы его не делать.
//
// Снятие отложенное, а не немедленное: удалить уже зарегистрированный
// перехватчик снаружи нельзя, поэтому он удаляет себя сам на первом же
// запросе, пришедшем после снятия.
func fenceHeartbeats(cluster *kfake.Cluster, code int16) func() {
	var armed atomic.Bool

	armed.Store(true)

	cluster.ControlKey(int16(kmsg.Heartbeat), func(req kmsg.Request) (kmsg.Response, error, bool) {
		if !armed.Load() {
			cluster.DropControl()

			return nil, nil, false
		}

		cluster.KeepControl()

		resp, ok := req.ResponseKind().(*kmsg.HeartbeatResponse)
		if !ok {
			return nil, nil, false
		}

		resp.ErrorCode = code

		return resp, nil, true
	})

	return func() { armed.Store(false) }
}

// TestPartitionsLostDoesNotCommit — потеря партиций не двигает оффсет.
//
// Ловит ровно одну регрессию: «добавим CommitMarkedOffsets в onPartitionsLost
// для симметрии с onPartitionsRevoked». Такая правка не роняет ни один другой
// тест, а стоит потери сообщений: партиции к этому моменту уже принадлежат
// другому участнику, и коммит, если координатор его примет, сдвинет оффсет за
// записи, которые новый владелец ещё не обработал.
//
// Потеря вызывается подменой ответа брокера на Heartbeat кодом
// UNKNOWN_MEMBER_ID. Выбран именно фатальный для сессии код: franz-go выходит
// из цикла heartbeat, минуя штатный отзыв, и зовёт onLost со всем назначением
// — а RebalanceInProgress привёл бы к обычному onPartitionsRevoked, то есть к
// соседней ветке. Двух консьюмеров с просроченной сессией тест не поднимает
// намеренно: там исход зависит от того, кто первым не успел, и это флак.
//
// Ассерт снимается снаружи: свежий консьюмер той же группы обязан получить
// сообщение заново. Внутреннее состояние (отметки, uncommitted) здесь не
// годится — franz-go чистит его сразу после колбэка, и по нему «не
// закоммитили» неотличимо от «закоммитили и забыли».
func TestPartitionsLostDoesNotCommit(t *testing.T) {
	t.Parallel()

	const (
		topic     = "kafkax-partition-loss-topic"
		processed = "processed-then-lost"
	)

	cluster, brokers := newFakeClusterHandle(t, 1, topic)

	logger, logged := spyLogger(t)
	cfg := testConfig(t, brokers...)
	cfg.Logger = logger
	// Автокоммит обязан не успеть: тикер закоммитил бы отмеченный оффсет
	// раньше потери партиций, и тест выродился бы в проверку автокоммита.
	cfg.Consumer.CommitInterval = time.Hour
	// Частый heartbeat — единственное, что приносит фатальный код и запускает
	// onLost; на умолчании в секунду тест ждал бы впустую.
	cfg.Consumer.HeartbeatInterval = 100 * time.Millisecond

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, processed)

	// Отказ на повторных доставках — не украшение сценария, а его условие.
	// После потери консьюмер переподключается и получает то же сообщение
	// снова; успешная обработка отметила бы оффсет, и финальный коммит внутри
	// Stop скрыл бы ровно то, что проверяется.
	h := &mockHandler{fn: func(call int, _ IncomingMessage) error {
		if call == 1 {
			return nil
		}

		return errConsBoom
	}}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	waitFor(t, consWait, "сообщение обработано, оффсет отмечен", func() bool {
		return h.callCount() == 1
	})

	// Подмена ставится только сейчас: до отметки оффсета терять нечего, и
	// колбэк отработал бы на пустом месте.
	disarm := fenceHeartbeats(cluster, kerr.UnknownMemberID.Code)

	waitFor(t, consWait, "консьюмер потерял партиции", func() bool {
		return logged.count(logPartitionsLost) > 0
	})

	// Штатного отзыва не было: иначе оффсет закоммитил бы он, и дальнейшая
	// проверка ловила бы чужой коммит, приняв его за коммит из onLost.
	if got := logged.count(logPartitionsRevoked); got != 0 {
		t.Fatalf("отзыв партиций отработал %d раз(а) до потери: сценарий проверяет не ту ветку", got)
	}

	// Брокер снова исправен: свежему консьюмеру из consDrainFresh иначе
	// доставалась бы та же подмена, и маркер он получал бы наперегонки с
	// собственной потерей партиций.
	disarm()

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	got := consDrainFresh(t, cfg, prod, topic, 0)

	want := []string{processed, consMarkerValue}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("свежий консьюмер получил %v, want %v: оффсет закоммичен при потере партиций", got, want)
	}
}
