package kafkax

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
)

const (
	consMetricPaused      = "kafkax.consumer.partitions.paused"
	consMetricWorkers     = "kafkax.consumer.workers.active"
	consMetricFetchErrors = "kafkax.consumer.fetch.errors"
	consMetricGroupErrors = "kafkax.consumer.group.errors"
)

// TestPoisonRaisesPausedGaugeAndDropsBufferedRecords — гейдж приостановленных
// партиций поднимается на отравлении и опускается вместе с воркером.
//
// Гейдж проверяется именно парой «поднялся — опустился». Инкремент без
// декремента незаметен в тесте, который смотрит только на факт отравления, но в
// проде означает алерт, который никогда не гаснет: набор пауз в franz-go живёт
// на уровне клиента и переживает и ребаланс, и отзыв партиции, поэтому снимать
// её обязан тот же код, что останавливает воркера.
//
// Заодно проверяется dropped: записи, набранные в очередь воркера до паузы,
// выбрасываются не отмеченными, и без счётчика масштаб отравления не виден —
// за одной остановившейся партицией стоит весь буфер, а не одно сообщение.
func TestPoisonRaisesPausedGaugeAndDropsBufferedRecords(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider
	const topic = "kafkax-paused-gauge-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, topic)
	cfg := testConfig(t, brokers...)

	prod := consNewProducer(t, brokers)
	// Две записи в одну партицию: первая травит, вторая обязана попасть в
	// очередь воркера до паузы и быть выброшенной. С одной записью ветка
	// dropped не выполняется вовсе.
	prod.send(t, topic, 0, consPoisonValue)
	prod.send(t, topic, 0, "behind-the-poison")

	h := &mockHandler{returnErr: errConsBoom}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	consWaitTerminal(t, rec, topic, consumerStatusError, 1)

	waitFor(t, consWait, "гейдж приостановленных партиций поднялся", func() bool {
		return rec.sum(consMetricPaused) == 1
	})

	waitFor(t, consWait, "запись за отравленной выброшена", func() bool {
		return rec.sum(consMetricProcessed,
			attribute.String("topic", topic),
			attribute.String("status", consumerStatusDropped)) == 1
	})

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Остановка воркера снимает паузу: партиции у нас больше нет, и держать за
	// неё поднятый гейдж значило бы алертить на чужую проблему.
	if got := rec.sum(consMetricPaused); got != 0 {
		t.Fatalf("partitions.paused = %d, want 0: пауза не снята вместе с воркером", got)
	}

	// Тот же инвариант для воркеров — если он сломается, гейдж пауз проверять
	// будет не по чему: оба живут на одном пути остановки.
	if got := rec.sum(consMetricWorkers); got != 0 {
		t.Fatalf("workers.active = %d, want 0", got)
	}

	// Ни одна из двух записей не отмечена — обе приедут снова.
	got := consDrainFresh(t, cfg, prod, topic, 0)

	want := []string{consPoisonValue, "behind-the-poison", consMarkerValue}
	if len(got) != len(want) {
		t.Fatalf("свежий консьюмер получил %v, want %v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("свежий консьюмер получил %v, want %v", got, want)
		}
	}
}

// TestPausedGaugeCountsPartitionsNotMessages — повторное отравление той же
// партиции не двигает гейдж.
//
// Воркер после паузы выбрасывает записи, но обвязка вокруг processRecord может
// упасть и на выброшенной. Без проверки «уже на паузе» счётчик уезжал бы вверх
// на каждой такой записи и никогда не возвращался: гейдж мерил бы сообщения
// вместо партиций, а декремент при остановке воркера — ровно один — оставлял бы
// его навсегда положительным.
func TestPausedGaugeCountsPartitionsNotMessages(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider
	const topic = "kafkax-paused-idempotent-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, topic)
	cfg := testConfig(t, brokers...)

	c := mustConsumer(t, cfg)

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	t.Cleanup(client.Close)

	key := workerKey{topic: topic, partition: 0}
	w := &partitionWorker{}

	// poison дёргается напрямую: воспроизводить второе падение обвязки поверх
	// уже отравленной партиции через kfake значило бы городить подмену
	// инструмента с ручным подсчётом вызовов ради одного if.
	log := &recordLogger{base: c.logger}

	c.poison(client, key, w, log, errConsBoom)
	c.poison(client, key, w, log, errConsBoom)

	if got := rec.sum(consMetricPaused); got != 1 {
		t.Fatalf("partitions.paused = %d, want 1: гейдж считает партиции, а не сообщения", got)
	}

	c.resumePartition(client, key)

	if got := rec.sum(consMetricPaused); got != 0 {
		t.Fatalf("partitions.paused = %d, want 0 после снятия паузы", got)
	}

	// Снятие паузы с партиции, которая на ней не стояла, не должно уводить
	// гейдж в минус: resumePartition вызывается на каждом создании воркера, то
	// есть подавляющее большинство вызовов приходится на здоровые партиции.
	c.resumePartition(client, key)

	if got := rec.sum(consMetricPaused); got != 0 {
		t.Fatalf("partitions.paused = %d, want 0: снятие несуществующей паузы ушло в минус", got)
	}
}

// TestFetchErrorsReportedOnStateChange — ошибки фетча считаются эпизодами.
//
// Неретраибельную ошибку franz-go оставляет в фетче, и следующий опрос приносит
// ту же самую: бэкоффа в этой ветке нет. Счётчик всех вхождений мерил бы частоту
// опроса, а лог давал бы поток записей Error — при MaxWait=500ms это минимум две
// в секунду на партицию. Инкремент делается на переходе «партиция была здорова →
// сломалась» и на смену текста ошибки.
//
// Через kfake неретраибельную партиционную ошибку не выписать, поэтому путь
// проверяется вызовом reportFetchError напрямую — вся логика дедупа живёт в нём.
func TestFetchErrorsReportedOnStateChange(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider
	const topic = "kafkax-fetch-dedup-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, topic)
	c := mustConsumer(t, testConfig(t, brokers...))

	topicAttr := attribute.String("topic", topic)
	first := errors.New("partition is broken")

	for range 5 {
		c.reportFetchError(topic, 0, first)
	}

	if got := rec.sum(consMetricFetchErrors, topicAttr); got != 1 {
		t.Fatalf("fetch.errors = %d, want 1: повтор той же ошибки — не новый эпизод", got)
	}

	// Смена причины отказа — новый эпизод: дежурному важно, что партиция
	// сломалась иначе, чем минуту назад.
	c.reportFetchError(topic, 0, errors.New("partition is broken differently"))

	if got := rec.sum(consMetricFetchErrors, topicAttr); got != 2 {
		t.Fatalf("fetch.errors = %d, want 2: смена текста ошибки не сообщена", got)
	}

	// Другая партиция того же топика ведёт свой учёт: журнал ключуется парой
	// топик+партиция, иначе поломка одной глушила бы сообщение о другой.
	c.reportFetchError(topic, 1, first)

	if got := rec.sum(consMetricFetchErrors, topicAttr); got != 3 {
		t.Fatalf("fetch.errors = %d, want 3: партиции учитываются раздельно", got)
	}

	// Партиция отдала записи — отметка снимается, и повторение той же ошибки
	// после выздоровления обязано быть сообщено заново. Без сброса дедуп
	// превратился бы в «сообщаем один раз за жизнь процесса».
	c.clearFetchError(workerKey{topic: topic, partition: 0})
	c.reportFetchError(topic, 0, errors.New("partition is broken differently"))

	if got := rec.sum(consMetricFetchErrors, topicAttr); got != 4 {
		t.Fatalf("fetch.errors = %d, want 4: выздоровевшая партиция не начала учёт заново", got)
	}
}

// TestFetchErrorsIgnoreShutdownNoise — отмена и закрытие клиента не считаются
// поломкой.
//
// Обе ошибки приходят на каждом штатном завершении процесса. Считай мы их,
// счётчик поломок рос бы на каждом деплое, и алерт по нему пришлось бы
// отключить — то есть он не сработал бы и на настоящей поломке.
func TestFetchErrorsIgnoreShutdownNoise(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider
	const topic = "kafkax-fetch-noise-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, topic)
	c := mustConsumer(t, testConfig(t, brokers...))

	c.reportFetchError(topic, 0, context.Canceled)
	c.reportFetchError(topic, 0, kgo.ErrClientClosed)

	if got := rec.sum(consMetricFetchErrors, attribute.String("topic", topic)); got != 0 {
		t.Fatalf("fetch.errors = %d, want 0: остановка — не поломка партиции", got)
	}
}

// TestGroupSessionErrorCountedSeparately — отказ уровня группы не смешивается с
// партиционным.
//
// franz-go подкидывает такую ошибку синтетическим фетчем с пустым топиком и
// партицией 0. Без разбора она выглядела бы в fetch.errors как поломка
// несуществующей партиции 0 топика "" — притом что это худший из отказов
// консьюмера: сообщений нет вообще, ни по одной партиции, и алерт на него нужен
// свой.
func TestGroupSessionErrorCountedSeparately(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider
	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, testTopic)
	c := mustConsumer(t, testConfig(t, brokers...))

	groupErr := &kgo.ErrGroupSession{Err: errors.New("session died")}

	for range 3 {
		c.reportFetchError("", 0, groupErr)
	}

	if got := rec.sum(consMetricGroupErrors); got != 1 {
		t.Fatalf("group.errors = %d, want 1: групповой отказ дедуплицируется как партиционный", got)
	}

	if got := rec.sum(consMetricFetchErrors); got != 0 {
		t.Fatalf("fetch.errors = %d, want 0: групповой отказ утёк в партиционный счётчик", got)
	}
}

// TestPoisonStopsFetchingFromPausedPartition — отравленная партиция перестаёт
// вычитываться с брокера, а не только выбрасываться воркером.
//
// Флага poisoned достаточно, чтобы записи не доходили до обработчика: воркер
// вычитывает их из очереди и считает как dropped. Поэтому вырезанный
// PauseFetchPartitions не ломает ни один тест, который смотрит на обработчик, а
// смысл самой паузы — не тянуть с брокера то, что всё равно будет выброшено —
// остаётся непроверенным. В проде это трафик и память на партицию, которая
// стоит; стоит она ровно столько, сколько дежурный разбирает инцидент, а не
// миллисекунды.
//
// Наблюдается это счётчиком dropped: он считает ровно те записи, которые
// консьюмер вытянул и выбросил, поэтому «выборка прекратилась» — это «dropped
// не вырос до размера непрочитанного хвоста». Верхняя граница берётся из
// ёмкости конвейера, а не с потолка: очередь воркера в один батч по одной
// записи плюс батч в руках цикла опроса — единицы записей против двух сотен в
// хвосте.
//
// Барьер, доказывающий, что клиент вообще продолжает ходить к брокеру, — это
// соседняя партиция того же топика. Проба, доехавшая до обработчика уже после
// паузы, означает, что источник собрал и отправил запрос выборки после неё, —
// а отравленная партиция попала бы в тот же запрос, не будь она снята с
// выборки. Ждать «ничего не приехало» по таймеру не понадобилось: у теста есть
// событие, наступающее строго после, а не спустя.
//
// Против kfake это видно так же, как против настоящей Kafka: пауза живёт в
// клиенте franz-go, и он не только не кладёт снятую партицию в запрос, но и
// вычёркивает её из уже набранного буфера.
func TestPoisonStopsFetchingFromPausedPartition(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider
	const (
		topic = "kafkax-paused-fetch-topic"
		// Хвост за отравившей записью. Настолько длиннее конвейера, что
		// «выбросили буфер» и «вычитали партицию до конца» не спутать.
		tail = 200
		// Потолок выброшенного: батч в обработке, батч в очереди и батч в руках
		// заблокированного цикла опроса — по одной записи каждый. Запас поверх
		// взят на планировщик, а не на «примерно столько».
		maxDropped = 10
		// Проб больше одной для запаса: каждая доказывает отдельный поход к
		// брокеру, случившийся уже после паузы.
		probes = 3
	)

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 2, topic)
	cfg := testConfig(t, brokers...)
	// Конвейер сжат до минимума: чем меньше записей успевает утечь в очередь
	// воркера до паузы, тем резче разница между «выбросили набранное» и
	// «вычитали хвост».
	cfg.Consumer.MaxPollRecords = 1
	cfg.Consumer.MessageQueueBatches = 1

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, consPoisonValue)

	for i := range tail {
		prod.send(t, topic, 0, fmt.Sprintf("tail-%d", i))
	}

	h := &mockHandler{fn: func(_ int, msg IncomingMessage) error {
		if string(msg.Value) == consPoisonValue {
			return errConsBoom
		}

		return nil
	}}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	waitFor(t, consWait, "отравленная партиция снята с выборки", func() bool {
		return rec.sum(consMetricPaused) == 1
	})

	for i := range probes {
		probe := fmt.Sprintf("probe-%d", i)
		prod.send(t, topic, 1, probe)

		waitFor(t, consWait, "проба соседней партиции доехала после паузы", func() bool {
			return consHasValue(h.messages(), probe)
		})
	}

	if got := rec.sum(consMetricProcessed,
		attribute.String("topic", topic),
		attribute.String("status", consumerStatusDropped)); got > maxDropped {
		t.Fatalf("dropped = %d при хвосте в %d записей, want <= %d: партиция не снята "+
			"с выборки и вычитывается в никуда", got, tail, maxDropped)
	}
}
