package kafkax

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Тесты консьюмера поверх внутрипроцессного брокера kfake.
//
// Проверяются не вызовы методов, а наблюдаемые гарантии: что сообщение доехало
// до обработчика целиком, что порядок внутри партиции не переставлен, что
// партиции идут параллельно и что оффсет закоммичен. Последнее нигде не
// читается напрямую: kadm сюда не тянется, вместо этого поднимается новый
// консьюмер в той же группе и проверяется, приедет ли сообщение повторно.
// Наличие или отсутствие повторной доставки — и есть то, что видит потребитель
// пакета.
//
// Почти все сценарии здесь параллельны: каждый поднимает собственный кластер
// kfake, поэтому одноимённые топик и группа соседей не пересекаются. Исключение
// — тесты, читающие метрики: captureMetrics подменяет глобальный
// otel.MeterProvider, и параллельный сосед писал бы в тот же журнал.

// Имена доменных метрик консьюмера. Продублированы здесь строками намеренно:
// в consumer.go это локальные переменные, и тест обязан ломаться, если имя
// метрики поменяют, — оно часть внешнего контракта, на нём построены дашборды.
const (
	consMetricProcessed = "kafkax.consumer.messages.processed"
	consMetricDuration  = "kafkax.consumer.message.duration"
	consMetricRetries   = "kafkax.consumer.handler.retries"
	consMetricPanics    = "kafkax.consumer.panics"
)

// consWait — общий потолок ожидания. Щедрый: сценарии упираются не в него, а в
// собственные условия, и завышенный потолок делает тест медленным только когда
// он и так падает.
const consWait = 20 * time.Second

// consMarkerValue — значение замыкающего сообщения, которым тесты отделяют
// «то, что группа не досчитала обработанным» от «того, что дописано после».
const consMarkerValue = "cons-marker"

// consProducer — продюсер с ручным выбором партиции.
//
// Публичный Producer выбирает партицию по хешу ключа, а половина сценариев
// здесь строится на том, что одна конкретная партиция встала, а соседняя
// работает. Подбирать ключи под нужную партицию — хрупко, поэтому сырой клиент
// franz-go с ManualPartitioner.
type consProducer struct {
	client *kgo.Client
}

func consNewProducer(t *testing.T, brokers []string) *consProducer {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient (тестовый продюсер): %v", err)
	}

	t.Cleanup(client.Close)

	return &consProducer{client: client}
}

// send синхронно кладёт value в конкретную партицию. Синхронно — чтобы порядок
// записей в логе совпадал с порядком вызовов send: на этом держатся проверки
// порядка и вся техника с маркером.
func (p *consProducer) send(t *testing.T, topic string, partition int32, value string) {
	t.Helper()

	rec := &kgo.Record{Topic: topic, Partition: partition, Value: []byte(value)}
	if err := p.client.ProduceSync(t.Context(), rec).FirstErr(); err != nil {
		t.Fatalf("ProduceSync(%s/%d, %q): %v", topic, partition, value, err)
	}
}

// consTrace — потокобезопасный журнал шагов: middleware и хуки дёргаются из
// горутин воркеров, а читает их тест из своей.
type consTrace struct {
	mu    sync.Mutex
	steps []string
}

func (tr *consTrace) add(step string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	tr.steps = append(tr.steps, step)
}

func (tr *consTrace) snapshot() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	return append([]string(nil), tr.steps...)
}

// consValues вытаскивает значения сообщений в порядке получения.
func consValues(msgs []IncomingMessage) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, string(m.Value))
	}

	return out
}

// consHasValue сообщает, доехало ли до обработчика сообщение с таким значением.
func consHasValue(msgs []IncomingMessage, value string) bool {
	for _, m := range msgs {
		if string(m.Value) == value {
			return true
		}
	}

	return false
}

// consHasAll проверяет, что получены все перечисленные значения. Дубликаты не
// мешают: гарантия пакета — at-least-once, и повторная доставка после ребаланса
// штатна.
func consHasAll(msgs []IncomingMessage, want []string) bool {
	got := make(map[string]struct{}, len(msgs))
	for _, m := range msgs {
		got[string(m.Value)] = struct{}{}
	}

	for _, v := range want {
		if _, ok := got[v]; !ok {
			return false
		}
	}

	return true
}

// consDrainFresh отвечает на единственный вопрос: закоммичен ли оффсет.
//
// Прямого способа спросить об этом у пакета нет, и это правильно — потребителя
// интересует не число в __consumer_offsets, а придёт ли сообщение снова.
// Поэтому в партицию дописывается маркер, поднимается НОВЫЙ консьюмер в той же
// группе, и возвращается всё, что он получил из этой партиции до маркера
// включительно. Пустой результат (кроме маркера) означает «оффсет закоммичен».
//
// Порядок внутри партиции гарантирован, поэтому маркер, приехавший первым,
// доказывает отсутствие повторной доставки, а не просто её задержку.
//
// Вызывать только после остановки прежнего консьюмера: иначе партиция
// останется за ним и свежий консьюмер не получит ничего.
func consDrainFresh(t *testing.T, cfg Config, prod *consProducer, topic string, partition int32) []string {
	t.Helper()

	prod.send(t, topic, partition, consMarkerValue)

	// Хуки исходного теста свежему консьюмеру не нужны: его задача — принять
	// всё незакоммиченное и не застрять на нём самому.
	fresh := cfg
	fresh.OnMessageSkipped = nil
	fresh.OnPanic = nil
	fresh.Consumer.HandlerMaxRetries = 0

	h := &mockHandler{}
	c := mustConsumer(t, fresh)
	mustAddHandler(t, c, topic, h)

	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start (свежий консьюмер): %v", err)
	}

	waitFor(t, consWait, "маркер доехал до свежего консьюмера", func() bool {
		return consHasValue(consPartitionMessages(h.messages(), partition), consMarkerValue)
	})

	// Свежий консьюмер обязан уйти из группы: следующий такой же вызов в том же
	// тесте иначе не получит партицию.
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop (свежий консьюмер): %v", err)
	}

	return consUpToMarker(consValues(consPartitionMessages(h.messages(), partition)))
}

// consPartitionMessages оставляет сообщения одной партиции.
func consPartitionMessages(msgs []IncomingMessage, partition int32) []IncomingMessage {
	out := make([]IncomingMessage, 0, len(msgs))

	for _, m := range msgs {
		if m.Partition == partition {
			out = append(out, m)
		}
	}

	return out
}

// consUpToMarker обрезает список по маркеру включительно.
func consUpToMarker(values []string) []string {
	for i, v := range values {
		if v == consMarkerValue {
			return values[:i+1]
		}
	}

	return values
}

// consStart запускает консьюмер на контексте теста и валит тест при отказе.
// Сценариям, которым нужна собственная отмена, Start вызывают сами.
//
// Консьюмер обязан быть создан через mustConsumer: t.Context() отменяется
// раньше, чем бегут t.Cleanup, и эта отмена запускает полноценную остановку в
// фоне. Дожидается её именно блокирующий Stop из cleanup'а mustConsumer — без
// него логи завершения и его обращения к фейковому брокеру пришлись бы на уже
// закончившийся тест.
func consStart(t *testing.T, c *Consumer) {
	t.Helper()

	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

// TestConsumerDeliversFullMessage проверяет, что до обработчика доезжает
// сообщение целиком, а не только тело.
//
// Поля Partition, Offset и Timestamp — не украшение: на них строится
// дедупликация в потребителе, которой обязывает at-least-once.
func TestConsumerDeliversFullMessage(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)

	p := mustProducer(t, cfg)
	if err := p.SendMessage(t.Context(), PublishRequest{
		Topic:   testTopic,
		Key:     []byte("the-key"),
		Value:   []byte("the-value"),
		Headers: Headers{{Key: "x-test-header", Value: []byte("hv")}},
	}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	h := &mockHandler{}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)
	consStart(t, c)

	waitFor(t, consWait, "сообщение доехало", func() bool { return h.callCount() == 1 })

	msg := h.messages()[0]

	if msg.Topic != testTopic {
		t.Errorf("Topic = %q, want %q", msg.Topic, testTopic)
	}

	if got := string(msg.Key); got != "the-key" {
		t.Errorf("Key = %q, want %q", got, "the-key")
	}

	if got := string(msg.Value); got != "the-value" {
		t.Errorf("Value = %q, want %q", got, "the-value")
	}

	if msg.Partition != 0 {
		t.Errorf("Partition = %d, want 0", msg.Partition)
	}

	if msg.Offset != 0 {
		t.Errorf("Offset = %d, want 0 (первое сообщение топика)", msg.Offset)
	}

	value, ok := msg.Headers.Get("x-test-header")
	if !ok || string(value) != "hv" {
		t.Errorf("Headers.Get(x-test-header) = %q, %v; want %q, true", value, ok, "hv")
	}

	// Точное значение задаёт брокер, поэтому проверяется только то, что метка
	// осмысленна: нулевой Timestamp означал бы, что поле просто не заполнено.
	if msg.Timestamp.IsZero() {
		t.Error("Timestamp пуст: поле не заполнено из записи")
	}

	if delta := time.Since(msg.Timestamp); delta < -time.Minute || delta > 5*time.Minute {
		t.Errorf("Timestamp = %v, отстоит от now на %v", msg.Timestamp, delta)
	}
}

// TestConsumerPreservesOrderWithinPartition — порядок внутри партиции.
//
// Ради него в пакете и заведена ровно одна горутина на партицию. Перестановка
// здесь означала бы, что параллелизм протёк внутрь партиции и порядок оффсетов
// больше ничего не значит.
func TestConsumerPreservesOrderWithinPartition(t *testing.T) {
	t.Parallel()

	const total = 25

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)

	prod := consNewProducer(t, brokers)

	want := make([]string, 0, total)
	for i := range total {
		v := fmt.Sprintf("m%02d", i)
		prod.send(t, testTopic, 0, v)
		want = append(want, v)
	}

	h := &mockHandler{}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)
	consStart(t, c)

	waitFor(t, consWait, "все сообщения доехали", func() bool { return h.callCount() == total })

	msgs := h.messages()
	got := consValues(msgs)

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("порядок нарушен: got %v, want %v", got, want)
		}
	}

	// Оффсеты обязаны идти строго вверх: обработка в порядке значений, но не в
	// порядке оффсетов означала бы совпадение, а не гарантию.
	for i := 1; i < len(msgs); i++ {
		if msgs[i].Offset <= msgs[i-1].Offset {
			t.Fatalf("оффсеты не возрастают: %d после %d", msgs[i].Offset, msgs[i-1].Offset)
		}
	}
}

// TestConsumerProcessesPartitionsInParallel — партиции идут параллельно.
//
// Обработчик первой партиции блокируется навсегда; если бы воркер был один,
// сообщение второй партиции не обработалось бы никогда, и тест повис бы на
// waitFor. Это и есть проверка: параллелизм между партициями — единственное,
// что здесь может её пройти.
func TestConsumerProcessesPartitionsInParallel(t *testing.T) {
	t.Parallel()

	const topic = "kafkax-parallel-topic"

	brokers := newFakeCluster(t, 2, topic)
	cfg := testConfig(t, brokers...)

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, "blocked")
	prod.send(t, topic, 1, "free")

	entered := make(chan struct{})
	release := make(chan struct{})

	var enterOnce sync.Once

	h := &mockHandler{fn: func(_ int, msg IncomingMessage) error {
		if msg.Partition == 0 {
			enterOnce.Do(func() { close(entered) })
			<-release
		}

		return nil
	}}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	select {
	case <-entered:
	case <-time.After(consWait):
		t.Fatal("обработчик партиции 0 так и не начал работу")
	}

	waitFor(t, consWait, "партиция 1 обработана, пока партиция 0 заблокирована", func() bool {
		return consHasValue(h.messages(), "free")
	})

	close(release)

	waitFor(t, consWait, "партиция 0 разблокирована и дообработана", func() bool {
		return consHasValue(h.messages(), "blocked")
	})
}

// TestConsumerRoutesTopicsToOwnHandlers — сообщение чужого топика не попадает
// в чужой обработчик.
//
// Обработчик выбирается по rec.Topic, а не по порядку регистрации: перепутанная
// маршрутизация означала бы, что данные одного потока обрабатываются логикой
// другого, и заметить это по метрикам невозможно.
func TestConsumerRoutesTopicsToOwnHandlers(t *testing.T) {
	t.Parallel()

	const (
		topicA = "kafkax-topic-a"
		topicB = "kafkax-topic-b"
	)

	brokers := newFakeCluster(t, 1, topicA, topicB)
	cfg := testConfig(t, brokers...)

	prod := consNewProducer(t, brokers)
	prod.send(t, topicA, 0, "value-a")
	prod.send(t, topicB, 0, "value-b")

	hA := &mockHandler{}
	hB := &mockHandler{}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topicA, hA)
	mustAddHandler(t, c, topicB, hB)
	consStart(t, c)

	waitFor(t, consWait, "оба топика обработаны", func() bool {
		return hA.callCount() == 1 && hB.callCount() == 1
	})

	for name, pair := range map[string]struct {
		h     *mockHandler
		topic string
		value string
	}{
		"A": {hA, topicA, "value-a"},
		"B": {hB, topicB, "value-b"},
	} {
		msgs := pair.h.messages()
		if len(msgs) != 1 {
			t.Fatalf("обработчик %s получил %d сообщений, want 1: %v", name, len(msgs), consValues(msgs))
		}

		if msgs[0].Topic != pair.topic || string(msgs[0].Value) != pair.value {
			t.Errorf("обработчик %s получил %s/%q, want %s/%q",
				name, msgs[0].Topic, msgs[0].Value, pair.topic, pair.value)
		}
	}
}

// TestConsumerMiddlewareOrder — первый переданный в AddHandler middleware
// оказывается внешним.
//
// Порядок — часть контракта Chain: логирование или метрики, оказавшиеся внутри
// фильтрующего middleware вместо снаружи, молча перестают видеть отфильтрованные
// сообщения.
func TestConsumerMiddlewareOrder(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)

	prod := consNewProducer(t, brokers)
	prod.send(t, testTopic, 0, "v")

	trace := &consTrace{}

	mw := func(name string) ConsumerMiddleware {
		return func(next ConsumerHandler) ConsumerHandler {
			return ConsumerHandlerFunc(func(ctx context.Context, msg IncomingMessage) error {
				trace.add("in:" + name)

				err := next.ProcessMessage(ctx, msg)

				trace.add("out:" + name)

				return err
			})
		}
	}

	h := &mockHandler{fn: func(int, IncomingMessage) error {
		trace.add("handler")

		return nil
	}}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h, mw("outer"), mw("inner"))
	consStart(t, c)

	waitFor(t, consWait, "сообщение прошло цепочку", func() bool { return h.callCount() == 1 })

	want := []string{"in:outer", "in:inner", "handler", "out:inner", "out:outer"}
	got := trace.snapshot()

	if len(got) != len(want) {
		t.Fatalf("цепочка = %v, want %v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("цепочка = %v, want %v", got, want)
		}
	}
}

// TestAddHandlerContract фиксирует фактический контракт AddHandler.
//
// Каждый отказ здесь — это ошибка, которую иначе пришлось бы ловить в рантайме
// в чужой горутине: nil в карте обработчиков, тихая замена обработчика,
// регистрация в топик, на который никто не подписан.
func TestAddHandlerContract(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)

	// Подтесты делят кластер, но не состояние: каждый заводит собственный
	// консьюмер, а cfg только читается. Единственный, кто доходит до Start, —
	// последний, так что за партиции они не соперничают.
	t.Run("пустой топик", func(t *testing.T) {
		t.Parallel()

		c := mustConsumer(t, cfg)
		if err := c.AddHandler("", &mockHandler{}); !errors.Is(err, ErrEmptyTopic) {
			t.Fatalf("AddHandler(\"\") = %v, want ErrEmptyTopic", err)
		}
	})

	t.Run("nil-обработчик", func(t *testing.T) {
		t.Parallel()

		c := mustConsumer(t, cfg)
		if err := c.AddHandler(testTopic, nil); !errors.Is(err, ErrNilHandler) {
			t.Fatalf("AddHandler(nil) = %v, want ErrNilHandler", err)
		}
	})

	t.Run("повторная регистрация топика — ошибка, а не замена", func(t *testing.T) {
		t.Parallel()

		c := mustConsumer(t, cfg)
		mustAddHandler(t, c, testTopic, &mockHandler{})

		err := c.AddHandler(testTopic, &mockHandler{})
		if err == nil {
			t.Fatal("повторная регистрация прошла молча: прежний обработчик заменён без следа")
		}

		if !errors.Is(err, ErrDuplicateHandler) {
			t.Fatalf("AddHandler (повтор) = %v, want ErrDuplicateHandler", err)
		}
	})

	t.Run("после Start", func(t *testing.T) {
		t.Parallel()

		c := mustConsumer(t, cfg)
		mustAddHandler(t, c, testTopic, &mockHandler{})
		consStart(t, c)

		// Топики уходят в kgo.ConsumeTopics при создании клиента, поэтому
		// зарегистрированный после Start обработчик остался бы без подписки.
		err := c.AddHandler("kafkax-late-topic", &mockHandler{})
		if !errors.Is(err, ErrConsumerStarted) {
			t.Fatalf("AddHandler после Start = %v, want ErrConsumerStarted", err)
		}
	})

	t.Run("после Stop", func(t *testing.T) {
		t.Parallel()

		c := mustConsumer(t, cfg)

		if err := c.Stop(); err != nil {
			t.Fatalf("Stop до Start = %v, want nil", err)
		}

		// Раньше эта регистрация проходила молча: флаг started остановленным
		// консьюмером не взводился. Вызывающий получал nil и полагал, что
		// подписка есть, — а Start после Stop уже невозможен, так что
		// обработчик не позвали бы никогда.
		err := c.AddHandler(testTopic, &mockHandler{})
		if !errors.Is(err, ErrConsumerClosed) {
			t.Fatalf("AddHandler после Stop = %v, want ErrConsumerClosed", err)
		}

		if errors.Is(err, ErrConsumerStarted) {
			t.Fatal("остановленный консьюмер выдал себя за запущенный")
		}
	})
}

// TestStartContract фиксирует фактический контракт Start.
func TestStartContract(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)

	// Подтесты проверяют только код возврата Start, поэтому одновременный вход
	// двух консьюмеров в одну группу им безразличен: доставку здесь не ждут.
	t.Run("без обработчиков и повтор после исправления", func(t *testing.T) {
		t.Parallel()

		c := mustConsumer(t, cfg)

		if err := c.Start(t.Context()); !errors.Is(err, ErrNoHandlers) {
			t.Fatalf("Start без обработчиков = %v, want ErrNoHandlers", err)
		}

		// Неуспешный Start обязан возвращать состояние в consumerIdle: иначе
		// исправить конфигурацию и повторить запуск было бы нельзя.
		mustAddHandler(t, c, testTopic, &mockHandler{})

		if err := c.Start(t.Context()); err != nil {
			t.Fatalf("Start после исправления = %v, want nil", err)
		}
	})

	t.Run("повторный Start", func(t *testing.T) {
		t.Parallel()

		c := mustConsumer(t, cfg)
		mustAddHandler(t, c, testTopic, &mockHandler{})
		consStart(t, c)

		if err := c.Start(t.Context()); !errors.Is(err, ErrConsumerStarted) {
			t.Fatalf("повторный Start = %v, want ErrConsumerStarted", err)
		}
	})

	t.Run("Start после Stop", func(t *testing.T) {
		t.Parallel()

		c := mustConsumer(t, cfg)
		mustAddHandler(t, c, testTopic, &mockHandler{})

		if err := c.Stop(); err != nil {
			t.Fatalf("Stop до Start = %v, want nil", err)
		}

		// Консьюмер, прошедший Stop, не перезапускается: клиент закрыт, а
		// молчаливый отказ вместо ошибки оставил бы приложение без потребителя.
		if err := c.Start(t.Context()); !errors.Is(err, ErrConsumerClosed) {
			t.Fatalf("Start после Stop = %v, want ErrConsumerClosed", err)
		}
	})

	t.Run("Start после полного цикла Start-Stop", func(t *testing.T) {
		t.Parallel()

		c := mustConsumer(t, cfg)
		mustAddHandler(t, c, testTopic, &mockHandler{})
		consStart(t, c)

		if err := c.Stop(); err != nil {
			t.Fatalf("Stop = %v, want nil", err)
		}

		// Ключевой случай RF-API-10. Пока состояние было булевым флагом
		// started, отработавший консьюмер оставался «запущенным» и на этот
		// Start отвечал ErrConsumerStarted — то есть предлагал ждать
		// несуществующий цикл опроса вместо создания нового консьюмера.
		err := c.Start(t.Context())
		if !errors.Is(err, ErrConsumerClosed) {
			t.Fatalf("Start после Start+Stop = %v, want ErrConsumerClosed", err)
		}

		if errors.Is(err, ErrConsumerStarted) {
			t.Fatal("остановленный консьюмер выдал себя за запущенный")
		}
	})
}

// TestConsumerSuccessMetrics — успешная обработка видна в метриках.
//
// Без status="success" в kafkax.consumer.messages.processed отличить работающий
// консьюмер от простаивающего по дашборду нельзя, а гистограмма длительности —
// единственный источник данных о задержке обработки.
func TestConsumerSuccessMetrics(t *testing.T) { //nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider: параллельный сосед смешал бы записи
	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)

	prod := consNewProducer(t, brokers)
	prod.send(t, testTopic, 0, "v")

	h := &mockHandler{}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)
	consStart(t, c)

	topicAttr := attribute.String("topic", testTopic)
	successAttr := attribute.String("status", consumerStatusSuccess)

	waitFor(t, consWait, "успех учтён в метрике", func() bool {
		return rec.sum(consMetricProcessed, topicAttr, successAttr) == 1
	})

	if got := rec.sum(consMetricProcessed, topicAttr, attribute.String("status", consumerStatusError)); got != 0 {
		t.Errorf("processed(status=error) = %d, want 0", got)
	}

	observations := rec.observations(consMetricDuration, topicAttr, successAttr)
	if len(observations) != 1 {
		t.Fatalf("наблюдений длительности = %d, want 1", len(observations))
	}

	// Секунды, а не миллисекунды: при усечении длительности до целых
	// миллисекунд всё быстрее миллисекунды падало бы в нулевую корзину.
	if observations[0] < 0 || observations[0] > 60 {
		t.Errorf("длительность = %v s, ожидалось разумное значение в секундах", observations[0])
	}
}

// Тесты ленивого логгера записи.
//
// Проверяется не текст сообщений, а момент, в который строится обогащённый
// логгер: happy path не пишет ни строки, и клонирование хэндлера на нём —
// чистая потеря. Считаются поэтому именно вызовы WithAttrs, а не байты вывода.

// loggerCalls — счётчики обращений к хэндлеру. Указатель общий у всех клонов:
// Logger.With порождает новый хэндлер, и без общего счётчика клонирование как
// раз и терялось бы из виду.
type loggerCalls struct {
	withAttrs int
	handled   int
}

// countingHandler считает клонирования и записи, ничего не выводя.
type countingHandler struct {
	calls *loggerCalls
}

func (h countingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h countingHandler) Handle(context.Context, slog.Record) error {
	h.calls.handled++

	return nil
}

func (h countingHandler) WithAttrs([]slog.Attr) slog.Handler {
	h.calls.withAttrs++

	return h
}

func (h countingHandler) WithGroup(string) slog.Handler { return h }

// testSpan — спан с заданным контекстом и без записи.
func testSpan(sampled bool) trace.Span {
	cfg := trace.SpanContextConfig{}
	if sampled {
		cfg = trace.SpanContextConfig{
			TraceID: trace.TraceID{
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
				0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
			},
			SpanID:     trace.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
			TraceFlags: trace.FlagsSampled,
		}
	}

	return trace.SpanFromContext(
		trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(cfg)))
}

// TestRecordLoggerBuildsNothingUntilUsed — happy path не платит за логгер.
func TestRecordLoggerBuildsNothingUntilUsed(t *testing.T) {
	t.Parallel()

	calls := &loggerCalls{}
	log := &recordLogger{base: slog.New(countingHandler{calls: calls}), offset: 7}
	log.span = testSpan(true)

	if calls.withAttrs != 0 {
		t.Fatalf("хэндлер клонирован %d раз до первого лога, want 0", calls.withAttrs)
	}

	log.get().Error("boom")

	// offset и trace_id — два обогащения, то есть два клона хэндлера.
	if calls.withAttrs != 2 {
		t.Errorf("клонов хэндлера = %d, want 2", calls.withAttrs)
	}

	if calls.handled != 1 {
		t.Errorf("записей = %d, want 1", calls.handled)
	}
}

// TestRecordLoggerCachesEnrichment — путь отказа логирует по несколько раз, и
// клонировать хэндлер заново на каждую строку незачем.
func TestRecordLoggerCachesEnrichment(t *testing.T) {
	t.Parallel()

	calls := &loggerCalls{}
	log := &recordLogger{base: slog.New(countingHandler{calls: calls}), offset: 7}
	log.span = testSpan(true)

	first := log.get()

	for range 3 {
		log.get().Warn("retry")
	}

	if second := log.get(); second != first {
		t.Errorf("повторный get построил новый логгер")
	}

	if calls.withAttrs != 2 {
		t.Errorf("клонов хэндлера = %d, want 2 (обогащение строится один раз)", calls.withAttrs)
	}
}

// TestRecordLoggerWithoutTrace — без спана и с невалидным спан-контекстом
// обогащение сводится к offset.
//
// Ветка с nil не гипотетическая: логгер строится раньше спана, потому что
// перехватчик паник обвязки регистрируется раньше — и паника в самом
// WithProcessSpan обязана попасть в лог, а не в nil-разыменование.
func TestRecordLoggerWithoutTrace(t *testing.T) {
	t.Parallel()

	tests := map[string]trace.Span{
		"спан ещё не создан":       nil,
		"невалидный спан-контекст": testSpan(false),
	}

	for name, span := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			calls := &loggerCalls{}
			log := &recordLogger{base: slog.New(countingHandler{calls: calls}), offset: 7}
			log.span = span

			log.get().Error("boom")

			if calls.withAttrs != 1 {
				t.Errorf("клонов хэндлера = %d, want 1 (только offset)", calls.withAttrs)
			}
		})
	}
}

// TestRecordLoggerAttributes — обогащение доезжает до записи целиком: и
// offset, и trace_id.
func TestRecordLoggerAttributes(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	log := &recordLogger{
		base:   slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})),
		offset: 42,
	}
	log.span = testSpan(true)

	log.get().Error("boom")

	out := buf.String()
	for _, want := range []string{`"offset":42`, `"trace_id":"0102030405060708090a0b0c0d0e0f10"`} {
		if !strings.Contains(out, want) {
			t.Errorf("в записи нет %s: %s", want, out)
		}
	}
}
