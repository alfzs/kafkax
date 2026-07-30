package kafkax

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Тесты бюджета времени одной отправки (RF-PERF-03).
//
// Проверяется контекст, который уходит в ProduceSync. Мокнуть тут нечего:
// p.client — конкретный *kgo.Client, поэтому дедлайн снимается с sendContext —
// единственного места, где этот контекст рождается, и единственного аргумента
// ProduceSync. Наблюдаемую часть — какую ошибку получит вызывающий —
// проверяет отдельный сценарий против настоящего брокера.

// TestProducerSendContextBudget проверяет, какой дедлайн доезжает до отправки.
//
// Главное здесь — первый подтест: когда дедлайн вызывающего раньше
// MessageTimeout, собственный контекст не создаётся вовсе, и в ProduceSync
// уходит ровно тот ctx, что пришёл снаружи.
func TestProducerSendContextBudget(t *testing.T) {
	t.Parallel()

	// Заметно больше, чем дедлайны вызывающих ниже, и заметно меньше, чем
	// умолчание в 30s: обе ветки должны выбираться однозначно.
	const messageTimeout = 10 * time.Second

	p := &Producer{messageTimeout: messageTimeout}

	t.Run("дедлайн вызывающего раньше — своего контекста нет", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		got, done := p.sendContext(ctx)
		defer done()

		// Сравнение самих контекстов, а не только дедлайнов: обёртка
		// context.WithCancel отдала бы тот же Deadline, но стоила бы ровно те
		// аллокации, ради снятия которых ветка и заведена.
		if got != ctx {
			t.Fatal("при более раннем дедлайне вызывающего создан собственный контекст отправки")
		}
	})

	t.Run("дедлайн вызывающего позже — бюджет режется", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
		defer cancel()

		got, done := p.sendContext(ctx)
		defer done()

		assertBudget(got, t, messageTimeout)
	})

	t.Run("дедлайна нет — бюджет MessageTimeout", func(t *testing.T) {
		t.Parallel()

		got, done := p.sendContext(t.Context())
		defer done()

		assertBudget(got, t, messageTimeout)
	})

	t.Run("дедлайн вызывающего уже истёк — отправка отменена сразу", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()

		got, done := p.sendContext(ctx)
		defer done()

		// Ветка «дедлайн раньше» захватывает и прошлое: пропуск обязан
		// сохранять отказ, а не отпускать истёкшую отправку в брокер с
		// десятисекундным бюджетом.
		if !errors.Is(got.Err(), context.DeadlineExceeded) {
			t.Fatalf("контекст отправки с истёкшим дедлайном: Err() = %v, want DeadlineExceeded", got.Err())
		}
	})
}

// assertBudget проверяет, что до дедлайна контекста осталось примерно want.
//
// Допуск односторонний и щедрый: между установкой дедлайна и проверкой
// проходит неизвестное время, но потратить его больше секунды на трёх
// присваиваниях нельзя ни на какой машине.
func assertBudget(ctx context.Context, t *testing.T, want time.Duration) {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("у контекста отправки нет дедлайна: бюджет MessageTimeout не поставлен")
	}

	if left := time.Until(deadline); left > want || left < want-time.Second {
		t.Fatalf("до дедлайна отправки %s, want ~%s", left, want)
	}
}

// TestProducerSendMessageHonoursCallerDeadline проверяет наблюдаемую часть
// правки: вызывающий с собственным дедлайном получает ту же ошибку, что и до
// неё.
//
// Ветка «своего контекста не создаём» меняет то, чей объект контекста доезжает
// до ProduceSync, и это ровно то место, где ошибка могла бы поменять класс:
// таймаут вызывающего обязан остаться ErrDeliveryTimeout с
// context.DeadlineExceeded под ним, а не превратиться в отмену.
//
//nolint:paralleltest // измеряет, что возврат укладывается в дедлайн вызывающего, а не в MessageTimeout
func TestProducerSendMessageHonoursCallerDeadline(t *testing.T) {
	// Брокера нет: отправка обязана висеть до дедлайна, а какого именно —
	// и есть предмет проверки.
	cfg := testConfig(t)
	cfg.Producer.MessageTimeout = 30 * time.Second

	p := mustProducer(t, cfg)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	start := time.Now()

	err := p.SendMessage(ctx, PublishRequest{Topic: testTopic, Value: []byte("v")})
	if !errors.Is(err, ErrDeliveryTimeout) {
		t.Fatalf("SendMessage(ctx с дедлайном 1s) = %v, want ErrDeliveryTimeout", err)
	}

	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, kgo.ErrRecordTimeout) {
		t.Errorf("ErrDeliveryTimeout пришёл без причины: %v", err)
	}

	// Сработал дедлайн вызывающего, а не MessageTimeout: иначе возврат занял бы
	// все тридцать секунд.
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("возврат занял %s — дедлайн вызывающего проигнорирован", elapsed)
	}
}
