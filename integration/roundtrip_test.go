package integration

import (
	"testing"

	"github.com/alfzs/kafkax/v3"
)

// TestRoundTrip — базовый круг против настоящего брокера: отправили, приняли,
// закоммитили, перезапустились, повторно не получили.
//
// Это первый тест набора, и его задача двойная. Во-первых, он проверяет самое
// дорогое утверждение пакета — что оффсет коммитится только после успешного
// возврата обработчика и переживает перезапуск процесса. Против kfake это уже
// проверено, но kfake хранит оффсеты в памяти собственного процесса и не знает
// ни о __consumer_offsets, ни о координаторе группы; здесь коммит проходит весь
// настоящий путь.
//
// Во-вторых, он служит проверкой самой обвязки: если сломан подъём контейнера,
// создание тем или конфигурация, красным станет этот тест, а не пять сценариев
// сразу, и разбирать будет нечего.
func TestRoundTrip(t *testing.T) {
	t.Parallel()

	topic := newTopic(t, 1)
	cfg := testConfig(t)

	producer, err := kafkax.NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	closeProducer(t, producer)

	for _, value := range []string{"first", "second", "third"} {
		if err := producer.SendMessage(t.Context(), kafkax.PublishRequest{
			Topic: topic,
			Value: []byte(value),
		}); err != nil {
			t.Fatalf("SendMessage(%s): %v", value, err)
		}
	}

	first := &collector{}
	consumer := startConsumer(t, cfg, topic, first)

	await(t, "первый консьюмер получил все три записи", func() bool {
		return first.count() >= 3
	})

	// Stop коммитит отмеченное, не полагаясь на тикер автокоммита.
	if err := consumer.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Свежий консьюмер той же группы: если коммит дошёл до брокера, читать ему
	// нечего. Маркер отделяет «ничего не приехало» от «консьюмер вообще не
	// заработал» — без него тест зеленел бы и на неподнявшемся консьюмере.
	second := &collector{}
	startConsumer(t, cfg, topic, second)

	if err := producer.SendMessage(t.Context(), kafkax.PublishRequest{
		Topic: topic,
		Value: []byte("marker"),
	}); err != nil {
		t.Fatalf("SendMessage(marker): %v", err)
	}

	await(t, "второй консьюмер получил маркер", func() bool {
		return second.has("marker")
	})

	if got := second.snapshot(); len(got) != 1 {
		t.Fatalf("второй консьюмер получил %v, want только маркер: "+
			"оффсет за обработанное не закоммичен", got)
	}
}

// startConsumer создаёт консьюмера, регистрирует обработчик и запускает его.
func startConsumer(
	t *testing.T, cfg kafkax.Config, topic string, handler kafkax.ConsumerHandler,
) *kafkax.Consumer {
	t.Helper()

	consumer, err := kafkax.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	if err := consumer.AddHandler(topic, handler); err != nil {
		t.Fatalf("AddHandler: %v", err)
	}

	if err := consumer.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Идемпотентен: явный Stop внутри теста этому не мешает.
	stopConsumer(t, consumer)

	return consumer
}
