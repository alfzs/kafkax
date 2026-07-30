package kafkax

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Обратное давление на очередь партиционного воркера в штатном режиме.
//
// Соседний файл про смерть воркера подходит к тем же веткам dispatch с другой
// стороны: там очередь никто не разбирает, потому что разбирать её некому.
// Здесь воркер жив и занят — очередь полна ровно потому, что обработка не
// поспевает за опросом, а это нормальный режим любого консьюмера под нагрузкой,
// а не отказ.
//
// Цена дефекта в этом режиме не «сообщение потерялось», а «участник выпал из
// группы»: цикл опроса стоит в dispatch с удерживаемым гейтом
// BlockRebalanceOnPoll, ребаланс ждать не будет, и координатор исключит
// участника по истечении бюджета. Партиции при этом уедут к соседям, а
// незакоммиченный хвост будет перечитан заново — то есть штатное торможение
// превратится в лавину перебалансировок.

// TestBackpressureOnLiveWorkerKeepsConsumerInGroup — полная очередь живого
// воркера тормозит опрос, но не выбрасывает консьюмера из группы.
//
// Класс дефекта: гейт, удерживаемый дольше бюджета ребаланса. Ветка
// `worker.records <- ftp.Records` в dispatch блокирующая и без таймаута
// (выброшенный батч был бы перепрыгнут коммитом следующего), поэтому занятый
// обработчик останавливает весь цикл опроса — вместе с гейтом, который franz-go
// держит от непустого PollRecords до AllowRebalance. Пока гейт поднят,
// координатор ждёт, а участник не отвечает.
//
// Синхронизация целиком на хуках franz-go, без единого sleep'а: сценарий держит
// одновременно полную очередь и незавершённый ребаланс, и по времени он
// воспроизводился бы через раз.
//   - pollWatch (HookFetchRecordUnbuffered) считает записи, вынутые из буфера
//     клиента, — по нему видно, что третья отправка уже началась;
//   - OnPartitionsCallbackBlocked — сигнал самого franz-go «ребаланс хочет
//     случиться, но его держит поллер». Он и есть доказательство, что гейт в
//     руках у застрявшего dispatch, а не гипотеза о нём.
//
// MessageQueueSize здесь рабочий параметр, а не строка конфига: одна ячейка при
// одном батче в опросе — минимальная конструкция, в которой третья запись
// упирается в полную очередь (первая у воркера, вторая в ячейке). С умолчанием
// в шестнадцать батчей все три улеглись бы в очередь, dispatch не встал бы, и
// ребаланс блокировать было бы нечем.
//
// Ассерты сняты с гейджа воркеров, а не с числа обработанных сообщений. Гейдж
// различает три исхода там, где счётчик сообщений видит один: 2 — ребаланс не
// начался, 1 — партиция отдана штатным отзывом и участник остался в группе,
// 0 — участника исключили и партиции пришли через onPartitionsLost. Именно
// последнее и есть проверяемый дефект, и отличить его от нормы можно только так.
//
//nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider: параллельный сосед смешал бы записи
func TestBackpressureOnLiveWorkerKeepsConsumerInGroup(t *testing.T) {
	const topic = "kafkax-live-backpressure-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 2, topic)
	watch := &pollWatch{}

	var blocked atomic.Int64

	cfg := testConfig(t, brokers...)
	// Один батч на опрос: без него три записи приехали бы одним батчем в одну
	// ячейку очереди, и второй отправки — той самой, что упирается в занятого
	// воркера, — не случилось бы вовсе.
	cfg.Consumer.MaxPollRecords = 1
	cfg.Consumer.MessageQueueSize = 1
	cfg.ExtraOpts = []kgo.Opt{
		kgo.WithHooks(watch),
		kgo.OnPartitionsCallbackBlocked(func(context.Context, *kgo.Client) { blocked.Add(1) }),
	}

	prod := consNewProducer(t, brokers)

	wedge := []string{"w0", "w1", "w2"}
	for _, v := range wedge {
		prod.send(t, topic, 0, v)
	}

	h := &queueGate{open: make(chan struct{})}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	waitFor(t, consWait, "обе партиции получили воркеров", func() bool {
		return rec.sum(consMetricWorkers) == 2
	})

	// Условие составное намеренно. Один polled без счётчика входов означал бы
	// только «записи вынуты из буфера»: они могли лежать в очереди, а воркер —
	// ещё не начать первую. Тогда полной очереди нет, и ждать ребаланс,
	// упёршийся в гейт, было бы бессмысленно.
	waitFor(t, consWait, "цикл опроса упёрся в полную очередь живого воркера", func() bool {
		return h.entered.Load() == 1 && watch.polled(topic) >= len(wedge)
	})

	// Снимок до появления второго участника: ребаланса ещё не было, и всё, что
	// хук насчитает дальше, вызвано именно им.
	before := blocked.Load()

	consJoinGroup(t, brokers, topic)

	waitFor(t, consWait, "ребаланс упёрся в гейт BlockRebalanceOnPoll", func() bool {
		return blocked.Load() > before
	})

	// Воркер жив и занят обработкой: очередь полна под нагрузкой, а не потому,
	// что разбирать её некому. Без этой проверки сценарий был бы неотличим от
	// соседнего, где dispatch упирается в мёртвого воркера.
	if got := h.entered.Load(); got != 1 {
		t.Fatalf("обработчик вызван %d раз, want 1: очередь полна не из-за занятого воркера", got)
	}

	// Ребаланс объявлен, но ни одна партиция ещё не отозвана — держит его
	// именно застрявший dispatch.
	if got := rec.sum(consMetricWorkers); got != 2 {
		t.Fatalf("workers.active = %d, want 2: партиция отозвана до того, как dispatch отпустил гейт", got)
	}

	close(h.open)

	waitFor(t, consWait, "консьюмер отдал одну партицию и остался в группе", func() bool {
		return rec.sum(consMetricWorkers) == 1
	})

	// Отзыв прошёл штатным путём. onPartitionsLost — тот же гейдж воркеров, но
	// другой исход: партиции забрали у исключённого участника, и отмеченные
	// оффсеты вместе с ними потеряны, потому что коммитить их уже некуда.
	if got := rec.sum(consMetricPartitionsLost); got != 0 {
		t.Fatalf("partitions.lost = %d, want 0: участника исключили из группы, пока dispatch ждал очередь", got)
	}

	if got := rec.sum(consMetricGroupErrors); got != 0 {
		t.Fatalf("group.errors = %d, want 0: сессия группы не пережила ожидания в dispatch", got)
	}

	// Остаться в группе мало — надо ещё и потреблять. Запись пишется в обе
	// партиции, потому что какая из двух досталась второму участнику, решает
	// балансировщик; консьюмеру принадлежит ровно одна, и ровно одну из этих
	// двух записей он обязан получить.
	prod.send(t, topic, 0, "alive-0")
	prod.send(t, topic, 1, "alive-1")

	waitFor(t, consWait, "консьюмер обработал запись на оставшейся за ним партиции", func() bool {
		return h.received("alive-0") || h.received("alive-1")
	})

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// consJoinGroup подсаживает в группу второго участника и тем самым запускает
// ребаланс.
//
// Сырой клиент franz-go, а не второй KafkaConsumer: тесту нужен только повод
// для ребаланса, а второй экземпляр пакета писал бы в тот же глобальный
// MeterProvider и смешал бы свои воркеры с проверяемыми. Опрос ему не нужен —
// группой franz-go управляет отдельной горутиной, поднятой ещё в NewClient.
//
// Пороги сессии оставлены умолчанию franz-go (они щедрее тестовых), поэтому
// бюджет ребаланса у группы берётся по нему: участник, застрявший в dispatch,
// падает из группы по решению теста, а не по срабатыванию таймаута.
func consJoinGroup(t *testing.T, brokers []string, topic string) {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(testGroup),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient (второй участник группы): %v", err)
	}

	t.Cleanup(client.Close)
}

// queueGate — обработчик, удерживающий воркера занятым до открытия ворот.
//
// От blockOnCancel из соседнего файла отличается тем, что воркер здесь остаётся
// живым и работоспособным: ворота открывает тест, а не отмена. Иначе после
// разблокировки некому было бы разобрать очередь и доказать, что консьюмер
// продолжает потреблять.
type queueGate struct {
	open    chan struct{}
	entered atomic.Int64

	mu   sync.Mutex
	seen []string
}

// ProcessMessage записывает сообщение до ожидания ворот: тесту важно «доехало
// до обработчика», а не «обработка завершилась».
func (g *queueGate) ProcessMessage(ctx context.Context, msg IncomingMessage) error {
	g.entered.Add(1)

	g.mu.Lock()
	g.seen = append(g.seen, string(msg.Value))
	g.mu.Unlock()

	select {
	case <-g.open:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *queueGate) received(value string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	return slices.Contains(g.seen, value)
}
