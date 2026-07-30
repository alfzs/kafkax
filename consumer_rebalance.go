package kafkax

// consumer_rebalance.go — колбэки ребаланса и остановка партиционных воркеров.
//
// Граница с соседями. Всё, что снимает воркера с карты и дожидается его
// выхода, живёт здесь — включая дренаж на Stop, которым пользуется shutdown в
// consumer.go. Создание воркеров и их внутренняя работа — в
// consumer_worker.go.

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// onPartitionsAssigned заводит воркеров назначенных партиций.
//
// Паузу снимает не этот колбэк, а создание воркера внутри c.worker — см.
// resumePartition. Балансировщик franz-go по умолчанию кооперативный, и
// assigned содержит только вновь добавленные партиции: снятие паузы по этому
// списку промахивалось бы мимо всех, кто остался за тем же экземпляром.
func (c *KafkaConsumer) onPartitionsAssigned(_ context.Context, client *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			c.worker(client, workerKey{topic: topic, partition: partition})
		}
	}

	c.logger.Info("Partitions assigned", slog.Any("partitions", assigned))
}

// onPartitionsRevoked останавливает воркеров отзываемых партиций и фиксирует
// их оффсеты.
//
// Колбэк блокирует ребаланс, и это ровно то, что нужно: пока он не вернулся,
// партиция не уедет к другому участнику группы.
//
// Коммит здесь обязателен, а не «на всякий случай»: собственный
// OnPartitionsRevoked отключает встроенный defaultRevoke franz-go вместе с его
// финальным синхронным коммитом, и без явного вызова отмеченные оффсеты
// потерялись бы вместе с сессией.
func (c *KafkaConsumer) onPartitionsRevoked(ctx context.Context, client *kgo.Client, revoked map[string][]int32) {
	drainCtx, cancelDrain := c.rebalanceBudget(ctx)
	c.stopWorkers(drainCtx, client, revoked)
	cancelDrain()

	// Отдельный бюджет, а не остаток от drainCtx: если воркеров пришлось
	// добивать по таймауту, тот контекст уже отменён, и коммит провалился бы
	// мгновенно — потеряв ровно те оффсеты, ради которых колбэк и написан.
	commitCtx, cancelCommit := c.rebalanceBudget(ctx)
	defer cancelCommit()

	if err := client.CommitMarkedOffsets(commitCtx); err != nil {
		// Наружу этот отказ вернуть некому — колбэк ребаланса зовёт franz-go, а
		// не приложение, — поэтому здесь лог обязателен, и рядом с ним счётчик:
		// проваленный коммит на revoke означает, что новый владелец перечитает
		// хвост, и алерт по нему строится, а по строке в логе нет.
		c.metrics.commitErrors.Add(context.WithoutCancel(ctx), 1,
			metric.WithAttributes(attribute.String("phase", phaseRevoke)))
		c.logger.Error("Failed to commit marked offsets on revoke",
			slog.Any("partitions", revoked),
			slog.Any("error", err))

		return
	}

	c.logger.Info("Partitions revoked", slog.Any("partitions", revoked))
}

// onPartitionsLost останавливает воркеров потерянных партиций.
//
// Коммита здесь нет намеренно: партиции потеряны вместе с сессией группы, и
// коммит либо будет отвергнут координатором, либо перезапишет оффсет,
// принадлежащий уже другому участнику.
func (c *KafkaConsumer) onPartitionsLost(ctx context.Context, client *kgo.Client, lost map[string][]int32) {
	ctx, cancel := c.rebalanceBudget(ctx)
	defer cancel()

	c.stopWorkers(ctx, client, lost)

	// Считаются партиции, а не события: одно событие уносит столько партиций,
	// сколько было назначено, и «потеряли одну из тридцати» от «потеряли все
	// тридцать» по счётчику событий не отличить.
	total := 0
	for _, parts := range lost {
		total += len(parts)
	}

	c.metrics.partitionsLost.Add(context.WithoutCancel(ctx), int64(total))
	c.logger.Warn("Partitions lost", slog.Any("partitions", lost))
}

// rebalanceBudget ограничивает время, которое колбэк ребаланса проводит в
// ожидании воркеров.
//
// franz-go передаёт в колбэки контекст жизни клиента, а не контекст ребаланса:
// он отменяется только при закрытии клиента. Без собственного дедлайна
// зависший обработчик держал бы колбэк дольше RebalanceTimeout, координатор
// исключил бы участника из группы, и вместо управляемого отзыва партиций
// случился бы onLost — то есть худший из двух исходов наступал бы сам собой.
func (c *KafkaConsumer) rebalanceBudget(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.config.Consumer.RebalanceTimeout)
}

// keyedWorker — воркер вместе со своим ключом: снимок, снятый с карты под
// мьютексом, чтобы ждать воркеров, уже не удерживая его.
type keyedWorker struct {
	key    workerKey
	worker *partitionWorker
}

// stopWorkers мягко останавливает воркеров перечисленных партиций и дожидается
// их выхода.
func (c *KafkaConsumer) stopWorkers(ctx context.Context, client *kgo.Client, partitions map[string][]int32) {
	c.workersMu.Lock()

	stopped := make([]keyedWorker, 0, len(partitions))

	for topic, parts := range partitions {
		for _, partition := range parts {
			key := workerKey{topic: topic, partition: partition}
			if w, ok := c.workers[key]; ok {
				delete(c.workers, key)

				stopped = append(stopped, keyedWorker{key: key, worker: w})
			}
		}
	}

	c.workersMu.Unlock()

	c.closeAndAwait(ctx, client, stopped)
}

// stopAllWorkers мягко останавливает всех воркеров.
func (c *KafkaConsumer) stopAllWorkers(ctx context.Context, client *kgo.Client) {
	c.workersMu.Lock()

	stopped := make([]keyedWorker, 0, len(c.workers))

	for key, w := range c.workers {
		stopped = append(stopped, keyedWorker{key: key, worker: w})
	}

	clear(c.workers)
	c.workersMu.Unlock()

	c.closeAndAwait(ctx, client, stopped)
}

// closeAndAwait закрывает очереди снятых с карты воркеров и дожидается их
// выхода.
//
// Сначала закрываются все очереди, и лишь потом идёт ожидание: иначе воркеры
// дренировались бы по очереди, а не параллельно.
func (c *KafkaConsumer) closeAndAwait(ctx context.Context, client *kgo.Client, stopped []keyedWorker) {
	for _, kw := range stopped {
		kw.worker.stop()
	}

	c.awaitWorkers(ctx, stopped)

	// Пауза снимается вместе с воркером, который её поставил. Набор пауз в
	// franz-go живёт на уровне клиента и переживает и ребаланс, и отзыв
	// партиции: не сняв её здесь, мы оставили бы гейдж partitions.paused
	// поднятым за партицию, которой у нас уже нет, — то есть подняли бы алерт
	// на чужую проблему. Партиция, вернувшаяся к нам позже, получит свежего
	// воркера и будет прочитана заново в любом случае.
	for _, kw := range stopped {
		c.resumePartition(client, kw.key)
	}
}

// awaitWorkers ждёт завершения воркеров.
//
// Воркер, не уложившийся в бюджет, отменяется жёстко: продолжать обработку
// партиции, которая уже отдана другому участнику группы, хуже, чем оборвать
// текущее сообщение — оно всё равно не отмечено и приедет снова. Но и после
// отмены его выхода приходится дождаться: живой воркер продолжает отмечать
// оффсеты и трогать клиента параллельно с финальным коммитом и закрытием.
func (c *KafkaConsumer) awaitWorkers(ctx context.Context, stopped []keyedWorker) {
	i := 0
	for ; i < len(stopped); i++ {
		if !waitClosed(ctx, stopped[i].worker.done) {
			break
		}
	}

	if i == len(stopped) {
		return
	}

	// Бюджет исчерпан. Отмена идёт по всем оставшимся сразу, и лишь потом —
	// ожидание в одном общем жёстком бюджете: по отдельности худший случай
	// умножался бы на число воркеров.
	pending := stopped[i:]

	hardCtx, cancel := context.WithTimeout(
		context.WithoutCancel(c.lifeCtx), c.config.Consumer.RebalanceTimeout)
	defer cancel()

	// Одно событие на исчерпанный бюджет, а не на воркера: бюджет общий, и
	// «не уложились» — это одно решение, принятое разом по всем оставшимся.
	// Сколько именно воркеров оборвано, видно в логе.
	c.metrics.drainTimeouts.Add(context.WithoutCancel(c.lifeCtx), 1,
		metric.WithAttributes(attribute.String("phase", phaseWorkers)))

	for _, kw := range pending {
		c.logger.Warn("Partition worker did not stop in time, cancelling",
			slog.String("topic", kw.key.topic),
			slog.Int("partition", int(kw.key.partition)))
		kw.worker.cancel()
	}

	for _, kw := range pending {
		if !waitClosed(hardCtx, kw.worker.done) {
			c.logger.Error("Partition worker is still running after hard cancellation",
				slog.String("topic", kw.key.topic),
				slog.Int("partition", int(kw.key.partition)))
		}
	}
}

// awaitPollLoop дожидается выхода цикла опроса, при необходимости отменяя его
// жёстко. false означает, что цикл не вышел и трогать ни карту воркеров, ни
// клиента нельзя.
//
// Жёсткая отмена — это lifeCancel: она отменяет контексты воркеров, из-за чего
// разблокируются и dispatch, упёршийся в полную очередь, и сам воркер, если он
// стоял на приёме. Не помочь она может только тогда, когда цикл висит в чужом
// коде, отмену не проверяющем, — в slog.Handler или в экспортёре метрик.
func (c *KafkaConsumer) awaitPollLoop(ctx context.Context) bool {
	if waitClosed(ctx, c.loopDone) {
		return true
	}

	c.logger.Warn("Poll loop did not stop within graceful timeout, cancelling",
		slog.Duration("timeout", c.config.GracefulTimeout))
	c.lifeCancel()

	hardCtx, cancel := context.WithTimeout(
		context.WithoutCancel(c.lifeCtx), c.config.Consumer.RebalanceTimeout)
	defer cancel()

	return waitClosed(hardCtx, c.loopDone)
}

// waitClosed ждёт закрытия канала; false означает исчерпание бюджета.
func waitClosed(ctx context.Context, done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}
