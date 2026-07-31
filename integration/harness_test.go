// Package integration проверяет kafkax против настоящей Kafka.
//
// Зачем отдельно от основного набора. Все 190+ тестов в корне репозитория идут
// против kfake — эмулятора протокола в памяти. Он быстрый, детерминированный и
// покрывает логику пакета, но целого класса вопросов не воспроизводит в
// принципе: реальные версии брокера, настоящий кооперативный ребаланс между
// процессами, коммит и продолжение с закоммиченного оффсета, поведение при
// падении брокера, SASL и TLS против живого сервера, усечение топика. Всё это
// проверяется здесь.
//
// Модуль отдельный (по образцу tools/): testcontainers тянет клиент Docker со
// всем деревом, и в основном go.mod он попал бы в граф модулей каждого
// потребителя пакета. Из корня `go test ./...` этот модуль не виден и Docker не
// требует.
package integration

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alfzs/kafkax/v2"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaImage — образ брокера. Версия закреплена: «latest» превратил бы отказ
// теста в вопрос «сломался пакет или обновился брокер», а именно этот вопрос
// интеграционный набор и должен исключать.
const kafkaImage = "confluentinc/confluent-local:7.6.1"

// startTimeout — потолок на поднятие контейнера. Щедрый: в холодном окружении
// сюда попадает выкачивание образа.
const startTimeout = 5 * time.Minute

// waitFor — общий потолок ожидания в сценариях. Настоящий брокер отвечает
// медленнее kfake, и большая часть бюджета уходит на ребаланс: у него свои
// таймауты на стороне координатора, которые тест ускорить не может.
const waitFor = 90 * time.Second

// shared — единственный брокер на весь прогон пакета.
//
// Один на всех, а не по контейнеру на тест: подъём Kafka занимает секунды, и
// поштучный запуск превратил бы набор в многоминутный. Изоляция обеспечивается
// иначе — уникальными именами тем и групп на тест (см. newTopic и newGroup),
// потому что именно они, а не адрес брокера, разделяют состояние.
var shared struct {
	once      sync.Once
	container *tckafka.KafkaContainer
	brokers   []string
	err       error
}

// TestMain поднимает брокер один раз и гасит его после всех тестов.
//
// Пропуск, а не отказ, когда Docker недоступен: набор обязан быть запускаемым
// на машине без Docker — иначе разработчик, у которого его нет, не сможет даже
// собрать пакет. В CI отсутствие Docker обязано быть отказом, поэтому там
// выставляется KAFKAX_INTEGRATION=required.
func TestMain(m *testing.M) {
	code := m.Run()

	terminateShared()

	os.Exit(code)
}

// terminateShared вынесен из TestMain отдельной функцией ради defer cancel():
// в самом TestMain он стоял бы над os.Exit и не выполнился бы никогда, оставив
// контекст течь до конца процесса.
func terminateShared() {
	if shared.container == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := shared.container.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "не удалось погасить контейнер: %v\n", err)
	}
}

// brokers отдаёт адреса поднятого брокера, поднимая его при первом обращении.
func brokers(t *testing.T) []string {
	t.Helper()

	shared.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
		defer cancel()

		shared.container, shared.err = tckafka.Run(ctx, kafkaImage,
			tckafka.WithClusterID("kafkax-integration"))
		if shared.err != nil {
			return
		}

		shared.brokers, shared.err = shared.container.Brokers(ctx)
	})

	if shared.err != nil {
		brokerUnavailable(t, shared.err)
	}

	return shared.brokers
}

// brokerUnavailable решает, чем считать не поднявшийся контейнер: отказом или
// причиной пропустить сценарий.
//
// Одна политика на весь набор, включая тесты со своим брокером (см.
// secureBrokers): разработчик без Docker обязан хотя бы собрать пакет, а в CI
// молчаливый пропуск равнозначен непроверенному коду — там выставляется
// KAFKAX_INTEGRATION=required, и любая причина, включая кривую конфигурацию
// брокера, становится красным тестом.
func brokerUnavailable(t *testing.T, err error) {
	t.Helper()

	if os.Getenv("KAFKAX_INTEGRATION") == "required" {
		t.Fatalf("брокер не поднялся, а KAFKAX_INTEGRATION=required: %v", err)
	}

	t.Skipf("Docker недоступен, сценарий пропущен: %v", err)
}

// newTopic создаёт тему с заданным числом партиций и уникальным именем.
//
// Имя выводится из имени теста, а не из счётчика: по упавшему прогону видно, к
// какому сценарию относится тема, оставшаяся в брокере. Тема не удаляется в
// Cleanup намеренно — брокер живёт только до конца прогона, а сохранённое
// состояние помогает разбирать отказ вручную.
func newTopic(t *testing.T, partitions int32) string {
	t.Helper()

	topic := topicName(t)
	createTopic(t, newAdmin(t), topic, partitions)

	return topic
}

// newTopicWith — то же, но с настройками уровня темы.
//
// Настройки темы — единственный способ заставить одиночного брокера повести
// себя как кластер под нагрузкой: min.insync.replicas=2 при RF=1 делает
// acks=-1 невыполнимым, не поднимая второй брокер, а max.message.bytes даёт
// отказ, который acks=0 обязан не заметить. Оба сценария проверяют настройку
// продюсера, а не брокера, поэтому и живут на общем брокере набора.
func newTopicWith(t *testing.T, partitions int32, configs map[string]*string) string {
	t.Helper()

	topic := topicName(t)
	createTopicWith(t, newAdmin(t), topic, partitions, 1, configs)

	return topic
}

// topicName отдаёт имя темы, уникальное для теста. Отдельно от newTopic ради
// сценариев со своим брокером: имя им нужно раньше, чем появляется админ, через
// которого тему можно создать.
func topicName(t *testing.T) string {
	t.Helper()

	return "it-" + sanitize(t.Name())
}

// createTopic создаёт тему у того брокера, к которому подключён admin.
func createTopic(t *testing.T, admin *kadm.Client, topic string, partitions int32) {
	t.Helper()

	createTopicWith(t, admin, topic, partitions, 1, nil)
}

// createTopicWith — то же с фактором репликации и настройками уровня темы.
//
// Отдельной функцией, а не двумя лишними аргументами у createTopic: RF=1 без
// настроек — умолчание всего набора, и таскать его по трём десяткам вызовов
// значило бы прятать редкий случай среди частого.
func createTopicWith(
	t *testing.T,
	admin *kadm.Client,
	topic string,
	partitions int32,
	replication int16,
	configs map[string]*string,
) {
	t.Helper()

	resp, err := admin.CreateTopics(t.Context(), partitions, replication, configs, topic)
	if err != nil {
		t.Fatalf("создание темы %s: %v", topic, err)
	}

	for _, created := range resp {
		if created.Err != nil {
			t.Fatalf("создание темы %s: %v", topic, created.Err)
		}
	}
}

// minISRConfig — настройка темы min.insync.replicas.
//
// Настройка нужна двум разным сценариям и означает в них противоположное. На
// одиночном брокере значение 2 при RF=1 делает acks=-1 невыполнимым — это
// зонд, отличающий acks=-1 от 0 и 1. На кластере из трёх узлов то же значение
// 2 при RF=3, наоборот, оставляет теме право жить после потери одного узла, а
// значение 3 делает её нетерпимой к потере — это зонд, доказывающий, что
// acks=-1 действительно ждёт ВСЕ синхронные реплики.
func minISRConfig(replicas int) map[string]*string {
	return map[string]*string{"min.insync.replicas": new(strconv.Itoa(replicas))}
}

// newGroup отдаёт имя группы, уникальное для теста: общая группа связала бы
// параллельные сценарии общим assignment'ом.
func newGroup(t *testing.T) string {
	t.Helper()

	return "it-group-" + sanitize(t.Name())
}

// newAdmin отдаёт административного клиента для операций, которых нет в
// публичном API пакета: создание тем, удаление записей, чтение оффсетов группы.
func newAdmin(t *testing.T) *kadm.Client {
	t.Helper()

	return newAdminAt(t, brokers(t))
}

// newAdminAt — то же, но к указанному брокеру. Нужен сценариям, поднимающим
// собственный контейнер: общий брокер набора им не адресат.
func newAdminAt(t *testing.T, seeds []string) *kadm.Client {
	t.Helper()

	return kadm.NewClient(rawClient(t, seeds))
}

// rawClient — клиент franz-go в обход публичного API пакета. Нужен там, где
// проверка требует того, чего kafkax намеренно не даёт: явного выбора партиции
// при записи и чтения темы мимо групп.
func rawClient(t *testing.T, seeds []string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()

	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(seeds...)}, opts...)...)
	if err != nil {
		t.Fatalf("клиент franz-go: %v", err)
	}

	t.Cleanup(client.Close)

	return client
}

// openProducer создаёт продюсера по конфигурации теста и закрывает его по его
// окончании.
func openProducer(t *testing.T, cfg kafkax.Config) *kafkax.Producer {
	t.Helper()

	producer, err := kafkax.NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	closeProducer(t, producer)

	return producer
}

// closeProducer гасит продюсера по окончании теста и НЕ проглатывает ошибку.
//
// Close возвращает FlushError с числом записей, которые остались недоставленными
// к концу бюджета. Выброшенная в `_`, такая ошибка превращает «продюсер не смог
// дослать хвост» в зелёный прогон: сценарии здесь сверяются с содержимым темы,
// и недосланное выглядит для них ровно как правильно не отправленное.
//
// t.Errorf, а не t.Fatalf: Cleanup идёт после тела теста, и обрывать на нём
// нечего — а Fatal из Cleanup ещё и не даёт отработать остальным.
func closeProducer(t *testing.T, producer *kafkax.Producer) {
	t.Helper()

	t.Cleanup(func() {
		if err := producer.Close(); err != nil {
			t.Errorf("закрытие продюсера: %v", err)
		}
	})
}

// stopConsumer останавливает консьюмера по окончании теста, не проглатывая
// ошибку. Идемпотентен на стороне пакета: Stop выполняет остановку один раз и
// затем отдаёт тот же результат, так что явный Stop внутри теста этому не
// мешает и второй раз ошибку не выдумывает.
func stopConsumer(t *testing.T, consumer *kafkax.Consumer) {
	t.Helper()

	t.Cleanup(func() {
		if err := consumer.Stop(); err != nil {
			t.Errorf("остановка консьюмера: %v", err)
		}
	})
}

// publishValues отправляет по сообщению на каждое значение, делая ключом само
// значение.
//
// Ключ не декоративен: именно он определяет партицию, а многопартиционные
// сценарии держатся на том, что поток растащен по всей теме, а не сложен подряд
// в одну партицию. SendMessage синхронен, поэтому порядок записей внутри
// партиции совпадает с порядком аргументов — на этом стоят проверки, читающие
// снимок обработчика как последовательность.
func publishValues(t *testing.T, producer *kafkax.Producer, topic string, values ...string) {
	t.Helper()

	for _, value := range values {
		if err := producer.SendMessage(t.Context(), kafkax.PublishRequest{
			Topic: topic,
			Key:   []byte(value),
			Value: []byte(value),
		}); err != nil {
			t.Fatalf("SendMessage(%s): %v", value, err)
		}
	}
}

// committedOffset читает закоммиченный группой оффсет партиции; -1 означает
// «коммита ещё нет».
//
// Утверждение о состоянии в брокере, а не о его последствиях: доставка отвечает
// на вопрос «что приедет дальше», оффсет — на вопрос «что группа считает
// сделанным», и отличить коммит не туда от коммита вовремя можно только вторым.
//
// Отсутствие оффсета — значение, а не отказ теста. Метод вызывается из await в
// цикле, и на раннем витке группы может не быть вовсе: координатор назначается
// лениво, при первом join. Отличить «ещё нет» от «уже никогда» можно только по
// тому, дождался ли вызывающий нужного числа до конца своего бюджета, а этот
// счёт ведёт он.
func committedOffset(t *testing.T, admin *kadm.Client, group, topic string, partition int32) int64 {
	t.Helper()

	offsets, err := admin.FetchOffsets(t.Context(), group)
	if err != nil {
		return -1
	}

	response, ok := offsets.Lookup(topic, partition)
	if !ok || response.Err != nil {
		return -1
	}

	return response.At
}

// sanitize приводит имя теста к допустимому в имени темы Kafka виду.
func sanitize(name string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, name)
}

// testConfig — конфигурация, указывающая на поднятый брокер.
//
// Таймауты короче умолчаний пакета, но заметно длиннее, чем в модульных тестах:
// настоящий координатор отвечает не мгновенно, а слишком агрессивный
// SessionTimeout сделал бы ребаланс источником ложных отказов.
func testConfig(t *testing.T) kafkax.Config {
	t.Helper()

	return configFor(t, brokers(t))
}

// configFor — та же конфигурация, но для произвольного адреса: тесты со своими
// настройками брокера поднимают отдельный контейнер и общий harness им не
// подходит.
func configFor(t *testing.T, addrs []string) kafkax.Config {
	t.Helper()

	cfg := kafkax.DefaultConfig()
	cfg.Brokers = addrs
	cfg.ClientID = "kafkax-integration"
	cfg.Logger = testLogger(t)
	cfg.GracefulTimeout = 20 * time.Second
	cfg.DialTimeout = 10 * time.Second
	// Логи franz-go на Warn: на Info настоящий брокер комментирует каждый
	// ребаланс и метаданные, и разбирать отказ в этом потоке нельзя.
	cfg.KafkaLogLevel = kafkax.KafkaLogWarn

	cfg.Consumer.Group = newGroup(t)
	cfg.Consumer.InitialOffset = kafkax.OffsetEarliest
	cfg.Consumer.SessionTimeout = 10 * time.Second
	cfg.Consumer.HeartbeatInterval = 2 * time.Second
	cfg.Consumer.RebalanceTimeout = 20 * time.Second
	cfg.Consumer.CommitInterval = time.Second
	cfg.Consumer.MaxWait = 200 * time.Millisecond

	cfg.Producer.MessageTimeout = 20 * time.Second
	cfg.Producer.FlushTimeout = 20 * time.Second

	return cfg
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()

	return slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// await опрашивает cond до истечения waitFor и валит тест, если условие так и
// не наступило. Опрос, а не канал: условия здесь складываются из состояния
// брокера, которое тест поштучно не контролирует.
func await(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(waitFor)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("не дождались: %s", what)
}

// collector — обработчик, складывающий значения полученных сообщений.
type collector struct {
	mu     sync.Mutex
	values []string
	fn     func(msg kafkax.IncomingMessage) error
}

func (c *collector) ProcessMessage(_ context.Context, msg kafkax.IncomingMessage) error {
	c.mu.Lock()
	c.values = append(c.values, string(msg.Value))
	fn := c.fn
	c.mu.Unlock()

	if fn != nil {
		return fn(msg)
	}

	return nil
}

func (c *collector) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]string(nil), c.values...)
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.values)
}

// has сообщает, встречалось ли значение хотя бы раз. Именно «хотя бы раз»:
// гарантия пакета — at-least-once, и требовать ровно одного вхождения значило
// бы проверять exactly-once, которого он не даёт.
func (c *collector) has(value string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return slices.Contains(c.values, value)
}

// logSpy — slog.Handler, запоминающий записи, которые в cfg.Logger пишут и сам
// пакет, и franz-go через kslog.
//
// Логгер, а не подменённый otel.SetMeterProvider: причина эпизода уезжает в оба
// канала одним и тем же значением, но провайдер метрик глобален на процесс, а
// тесты набора идут параллельно — подмена глобали связала бы независимые
// сценарии и сделала бы отказ невоспроизводимым поодиночке. cfg.Logger же
// принадлежит одному клиенту.
type logSpy struct {
	store *logSpyStore
	inner slog.Handler
}

// logSpyStore разделяется всеми производными хендлерами: пакет навешивает на
// логгер component и group через With, а WithAttrs обязан вернуть новый
// хендлер — без общего хранилища записи уехали бы в копию.
type logSpyStore struct {
	mu      sync.Mutex
	entries []logSpyEntry
}

type logSpyEntry struct {
	level   slog.Level
	message string
	attrs   map[string]string
}

func newLogSpy(t *testing.T) *logSpy {
	t.Helper()

	return &logSpy{store: &logSpyStore{}, inner: testLogger(t).Handler()}
}

// Enabled пропускает всё. Порог для записей franz-go пакет применяет своей
// обёрткой НАД этим хендлером (Config.KafkaLogLevel), поэтому фильтр здесь
// означал бы, что тест видит меньше, чем видит логгер потребителя.
func (h *logSpy) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *logSpy) Handle(ctx context.Context, record slog.Record) error {
	entry := logSpyEntry{
		level:   record.Level,
		message: record.Message,
		attrs:   make(map[string]string, record.NumAttrs()),
	}

	record.Attrs(func(attr slog.Attr) bool {
		entry.attrs[attr.Key] = attr.Value.String()

		return true
	})

	h.store.mu.Lock()
	h.store.entries = append(h.store.entries, entry)
	h.store.mu.Unlock()

	// Записи всё равно уходят в лог теста: отказ разбирать по одним ассертам
	// нечем, а поток клиента — единственное, что о нём известно.
	if h.inner.Enabled(ctx, record.Level) {
		return h.inner.Handle(ctx, record)
	}

	return nil
}

func (h *logSpy) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logSpy{store: h.store, inner: h.inner.WithAttrs(attrs)}
}

func (h *logSpy) WithGroup(name string) slog.Handler {
	return &logSpy{store: h.store, inner: h.inner.WithGroup(name)}
}

func (h *logSpy) snapshot() []logSpyEntry {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	return append([]logSpyEntry(nil), h.store.entries...)
}

// contains сообщает, встречалась ли запись с подстрокой в тексте.
func (h *logSpy) contains(substring string) bool {
	for _, entry := range h.snapshot() {
		if strings.Contains(entry.message, substring) {
			return true
		}
	}

	return false
}

// freeHostPort отдаёт заведомо свободный номер порта.
//
// Окно между освобождением порта и привязкой его контейнером открыто, и
// закрывается оно только вместе с возможностью закрепить порт вообще: docker
// умеет принимать номер, но не умеет принимать уже открытый сокет. Константа в
// исходнике сталкивалась бы с чужим процессом несравнимо чаще.
//
// Окно шире, чем кажется, и это стоит помнить при разборе отказа. Оно
// открывается не один раз, а на каждом `docker start`, в том числе на том, что
// делает сам сценарий; номер приходит из эфемерного диапазона ядра, из которого
// одновременно раздаются и исходящие порты — а во время аварии клиенты под
// тестом переподключаются к localhost непрерывно. Отказ этого рода приходит
// внятной ошибкой docker'а («port is already allocated») из dedicatedBroker или
// startBroker и на утверждения теста не влияет.
func freeHostPort(t *testing.T) int {
	t.Helper()

	var lc net.ListenConfig

	listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("выбор свободного порта: %v", err)
	}

	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("неожиданный тип адреса %T", listener.Addr())
	}

	if err := listener.Close(); err != nil {
		t.Fatalf("освобождение порта: %v", err)
	}

	return addr.Port
}

// loadRunner — фоновая отправка, идущая через всю аварию.
//
// Нужна ровно за тем, чтобы перезапуск случился под нагрузкой, а не на
// простаивающем клиенте: продюсер с непустым буфером и консьюмер с непустой
// очередью переживают обрыв иначе, чем бездействующие.
type loadRunner struct {
	stopOnce sync.Once
	stopCh   chan struct{}
	done     chan struct{}

	// mu защищает acked. Сценарий смены лидера сверяет число подтверждений
	// до и после перевыборов, то есть читает поле, пока горутина в него
	// пишет; без замка это была бы гонка, а не наблюдение.
	mu sync.Mutex
	// acked — значения подтверждённых брокером отправок. Значения, а не
	// счётчик: «ни одна подтверждённая запись не потерялась» — утверждение о
	// содержимом темы, и числом его не проверить.
	acked []string
}

// stop останавливает нагрузку и дожидается её выхода, возвращая число
// подтверждённых отправок. Идемпотентен: зовётся и из теста, и из Cleanup.
func (l *loadRunner) stop() int {
	l.stopOnce.Do(func() { close(l.stopCh) })
	<-l.done

	return l.ackedCount()
}

// ackedCount отдаёт число подтверждённых отправок на текущий момент; вызывать
// можно и на идущей нагрузке.
func (l *loadRunner) ackedCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.acked)
}

// ackedValues отдаёт снимок подтверждённых отправок.
func (l *loadRunner) ackedValues() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.acked...)
}

func startLoad(t *testing.T, producer *kafkax.Producer, topic string) *loadRunner {
	t.Helper()

	load := &loadRunner{stopCh: make(chan struct{}), done: make(chan struct{})}

	go func() {
		defer close(load.done)

		// Пауза между отправками — регулятор темпа, а не часть доказательства:
		// на исправном брокере нагрузка иначе наливает десятки тысяч записей
		// за время подъёма контейнера, и вычитывание темы в конце теста
		// становится дороже самого сценария.
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for n := 0; ; n++ {
			select {
			case <-load.stopCh:
				return
			case <-ticker.C:
			}

			// Отказ отправки здесь штатен: часть попыток приходится ровно на
			// то время, когда брокера нет. Учитываются только подтверждённые.
			value := fmt.Sprintf("load-%d", n)

			err := producer.SendMessage(t.Context(), kafkax.PublishRequest{
				Topic: topic,
				Value: []byte(value),
			})
			if err == nil {
				load.mu.Lock()
				load.acked = append(load.acked, value)
				load.mu.Unlock()
			}
		}
	}()

	t.Cleanup(func() { load.stop() })

	return load
}

// readTopic вычитывает тему целиком мимо групп: это список того, что брокер
// действительно сохранил, в отличие от списка того, что тест пытался отправить.
func readTopic(t *testing.T, seeds []string, topic string) []string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()

	ends, err := newAdminAt(t, seeds).ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("границы темы %s: %v", topic, err)
	}

	if err := ends.Error(); err != nil {
		t.Fatalf("границы темы %s: %v", topic, err)
	}

	var total int64

	ends.Each(func(offset kadm.ListedOffset) { total += offset.Offset })

	if total == 0 {
		return nil
	}

	client := rawClient(t, seeds,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))

	values := make([]string, 0, total)

	for int64(len(values)) < total {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("чтение темы %s: %v", topic, err)
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			values = append(values, string(rec.Value))
		})
	}

	return values
}
