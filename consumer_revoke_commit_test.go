package kafkax

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel/attribute"
)

// Провал коммита на отзыве партиций.
//
// Вторая из двух веток `CommitMarkedOffsets`. Первая — финальный коммит в Stop —
// возвращает отказ вызывающему, и её проверяет TestStopReportsFailedFinalCommit.
// Отсюда вернуть отказ некуда: колбэк ребаланса зовёт franz-go, а не приложение.
// Поэтому единственный машиночитаемый след отказа — счётчик, и без теста на него
// ветка снова стала бы наблюдаемой только строкой в логе.

// failOffsetCommits заставляет кластер отвечать на каждый OffsetCommit кодом
// NOT_COORDINATOR.
//
// Отказ инжектируется на уровне протокола, а не гашением кластера, потому что
// гашение до этой ветки не доходит: на мёртвом брокере сессия группы обрывается
// с ошибкой сети, и franz-go уводит партиции в onPartitionsLost, где коммита
// нет намеренно. NOT_COORDINATOR даёт ту же причину («координатор больше не
// наш») при живой группе — то есть при ребалансе, который до отзыва партиций
// вообще доживает.
//
// Ответ собирается по запросу, а не возвращается ошибкой транспорта: kfake на
// ошибку рвёт соединение, и до кода отказа коммита дело бы не дошло — ветка
// проверялась бы на «сеть упала», хотя проверять нужно отказ, пришедший от
// живого координатора.
func failOffsetCommits(cluster *kfake.Cluster) {
	cluster.ControlKey(kmsg.OffsetCommit.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()

		commit, ok := req.(*kmsg.OffsetCommitRequest)
		if !ok {
			return nil, nil, false
		}

		resp, ok := commit.ResponseKind().(*kmsg.OffsetCommitResponse)
		if !ok {
			return nil, nil, false
		}

		resp.Version = commit.Version

		for _, topic := range commit.Topics {
			respTopic := kmsg.NewOffsetCommitResponseTopic()
			respTopic.Topic = topic.Topic

			for _, partition := range topic.Partitions {
				respPartition := kmsg.NewOffsetCommitResponseTopicPartition()
				respPartition.Partition = partition.Partition
				respPartition.ErrorCode = kerr.NotCoordinator.Code
				respTopic.Partitions = append(respTopic.Partitions, respPartition)
			}

			resp.Topics = append(resp.Topics, respTopic)
		}

		return resp, nil, true
	})
}

// TestRevokeReportsFailedCommitInMetric — провал коммита при отзыве партиций
// виден счётчиком, а не только записью в логе.
//
// Класс дефекта: отказ, о котором знает только журнал. Отзыв партиций —
// единственный момент, когда отмеченные оффсеты передаются следующему
// владельцу; провалившись здесь, коммит оставляет хвост партиции
// незакоммиченным, и новый владелец перечитает его целиком. Для потребителя это
// пачка дубликатов на каждом ребалансе — то есть на каждой выкатке. Строка в
// логе поднимает алерт только там, где логи индексируются и по ним написано
// правило; счётчик есть везде, поэтому доказывать нужно именно его.
//
// Ребаланс вызывается вторым консьюмером той же группы: это единственный отзыв
// партиций, который не смешивается с остановкой. На нём и держится второй
// ассерт — phase=shutdown обязан остаться нулём. Без него тест не отличал бы
// ветку отзыва от ветки Stop: обе инкрементят один счётчик и различаются только
// атрибутом, а перепутанный атрибут развесил бы алерт не на то событие.
//
//nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider
func TestRevokeReportsFailedCommitInMetric(t *testing.T) {
	const topic = "kafkax-revoke-commit-topic"

	rec := captureMetrics(t)

	cluster, brokers := newFakeClusterHandle(t, 2, topic)
	failOffsetCommits(cluster)

	cfg := testConfig(t, brokers...)
	// Автокоммит не должен успеть: коммитить нужно именно из колбэка отзыва, а
	// не тикером до него — иначе отмечать к моменту ребаланса было бы нечего,
	// franz-go вернул бы nil, не сходив к брокеру, и ветка не отработала бы.
	cfg.Consumer.CommitInterval = time.Hour
	// Умолчания растянули бы вход второго консьюмера в группу на секунды.
	cfg.Consumer.SessionTimeout = time.Second
	cfg.Consumer.HeartbeatInterval = 200 * time.Millisecond
	cfg.Consumer.RebalanceTimeout = 2 * time.Second

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "p0")
	prod.send(t, topic, 1, "p1")

	hFirst := &mockHandler{}
	first := mustConsumer(t, cfg)
	mustAddHandler(t, first, topic, hFirst)
	consStart(t, first)

	// Обе партиции обработаны и отмечены: отзыв произойдёт с непустым набором
	// оффсетов, иначе коммит вернул бы nil и ветка снова осталась бы непройденной.
	waitFor(t, consWait, "первый консьюмер обработал обе партиции", func() bool {
		return consHasValue(hFirst.messages(), "p0") && consHasValue(hFirst.messages(), "p1")
	})

	revokeAttr := attribute.String("phase", phaseRevoke)
	shutdownAttr := attribute.String("phase", phaseShutdown)

	// Точка отсчёта: до входа второго консьюмера в группу отзыва не было.
	if got := rec.sum(consMetricCommitErrors, revokeAttr); got != 0 {
		t.Fatalf("commit.errors{phase=%s} = %d до ребаланса, want 0", phaseRevoke, got)
	}

	hSecond := &mockHandler{}
	second := mustConsumer(t, cfg)
	mustAddHandler(t, second, topic, hSecond)
	consStart(t, second)

	waitFor(t, consWait, "проваленный коммит на отзыве посчитан", func() bool {
		return rec.sum(consMetricCommitErrors, revokeAttr) >= 1
	})

	// Оба консьюмера ещё работают, Stop никто не звал: счётчик вырос из ветки
	// отзыва, а не из финального коммита.
	if got := rec.sum(consMetricCommitErrors, shutdownAttr); got != 0 {
		t.Fatalf("commit.errors{phase=%s} = %d, want 0: отказ засчитан не той фазе", phaseShutdown, got)
	}
}
