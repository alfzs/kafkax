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
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alfzs/kafkax/v2"
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

	if shared.container != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		if err := shared.container.Terminate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "не удалось погасить контейнер: %v\n", err)
		}
	}

	os.Exit(code)
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

	topic := "it-" + sanitize(t.Name())

	admin := newAdmin(t)

	resp, err := admin.CreateTopics(t.Context(), partitions, 1, nil, topic)
	if err != nil {
		t.Fatalf("создание темы %s: %v", topic, err)
	}

	for _, created := range resp {
		if created.Err != nil {
			t.Fatalf("создание темы %s: %v", topic, created.Err)
		}
	}

	return topic
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

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers(t)...))
	if err != nil {
		t.Fatalf("административный клиент: %v", err)
	}

	t.Cleanup(client.Close)

	return kadm.NewClient(client)
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

	for _, got := range c.values {
		if got == value {
			return true
		}
	}

	return false
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
