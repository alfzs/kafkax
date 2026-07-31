// Примеры из README, положенные под компилятор.
//
// Пакет здесь внешний (kafkax_test, а не kafkax) намеренно: примеры обязаны
// видеть ровно то же, что видит потребитель, и ни байтом больше. Внутрипакетный
// пример прошёл бы и на непубличном имени, то есть доказывал бы не то.
//
// Ни у одного примера нет комментария Output, поэтому go test их компилирует,
// но не запускает: адрес kafka:9092 в них ненастоящий, а ценность здесь — сам
// факт сборки. Разъедется README с сигнатурами — это упадёт компилятором, а не
// обнаружится читателем.
package kafkax_test

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/alfzs/kafkax/v3"
)

// ExampleNewProducer — быстрый старт продюсера из README.
func ExampleNewProducer() {
	ctx := context.Background()

	cfg := kafkax.Config{
		Brokers:         []string{"kafka:9092"},
		ClientID:        "my-service",
		GracefulTimeout: 3 * time.Minute,
		DialTimeout:     10 * time.Second,
		Producer: kafkax.ProducerConfig{
			RequiredAcks:      -1,
			EnableIdempotence: true,
			MessageTimeout:    30 * time.Second,
			FlushTimeout:      time.Minute,
		},
	}

	producer, err := kafkax.NewProducer(cfg)
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close() //nolint:errcheck // пример из README; разбор ошибки Close показан в ExampleProducer_Close

	err = producer.SendMessage(ctx, kafkax.PublishRequest{
		Topic: "orders",
		Key:   []byte("order-1"),
		Value: []byte("payload"),
		Headers: kafkax.Headers{
			{Key: "signature", Value: []byte("signature")},
		},
	})
	// Не log.Fatal: отказ одной отправки — не повод уводить процесс, да и
	// отложенный Close при выходе уже не отработал бы.
	if err != nil {
		log.Print(err)
	}
}

// ExampleDeliveryError — разбор ошибки отправки ровно тем способом, который
// обещан наружу: сентинелы отвечают «повторять ли», DeliveryError — «что
// именно случилось». Типов franz-go здесь нет и быть не должно.
func ExampleDeliveryError() {
	producer, err := kafkax.NewProducer(kafkax.Config{
		Brokers:  []string{"kafka:9092"},
		ClientID: "my-service",
	})
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close() //nolint:errcheck // пример из README

	err = producer.SendMessage(context.Background(), kafkax.PublishRequest{
		Topic: "orders",
		Value: []byte("payload"),
	})

	switch {
	case err == nil:
		return
	case errors.Is(err, kafkax.ErrReservedHeaderKey):
		log.Print("заголовок занят W3C trace propagation, повтор бессмыслен")
	case errors.Is(err, kafkax.ErrProducerClosed):
		log.Print("сообщение точно не ушло, повтор безопасен")
	case errors.Is(err, kafkax.ErrDeliveryTimeout):
		log.Print("могло уйти, повтор способен создать дубликат")
	case errors.Is(err, kafkax.ErrDeliveryFailed):
		log.Print("брокер отверг")
	}

	if delivery, ok := errors.AsType[*kafkax.DeliveryError](err); ok {
		log.Printf("topic=%s retriable=%v", delivery.Topic, delivery.Retriable)
	}
}

// orderHandler — обработчик из README.
type orderHandler struct{}

func (h *orderHandler) ProcessMessage(ctx context.Context, msg kafkax.IncomingMessage) error {
	// ctx содержит OTel-span — его и передавать дальше в дочерние операции.
	if _, ok := msg.Headers.Get("signature"); ok {
		_ = ctx
	}

	return nil
}

// ExampleNewConsumer — быстрый старт консьюмера из README, включая порядок
// «обработчики до Start» и три состояния жизненного цикла.
func ExampleNewConsumer() {
	ctx := context.Background()

	cfg := kafkax.Config{
		Brokers:  []string{"kafka:9092"},
		ClientID: "my-service",
		Consumer: kafkax.ConsumerConfig{
			Group:             "my-service.group",
			InitialOffset:     kafkax.OffsetEarliest,
			SessionTimeout:    45 * time.Second,
			HeartbeatInterval: 3 * time.Second,
			RebalanceTimeout:  time.Minute,
			CommitInterval:    5 * time.Second,
			HandlerRetries:    3,
			HandlerRetryDelay: time.Second,
		},
	}

	consumer, err := kafkax.NewConsumer(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Подписка происходит внутри Start, отдельного шага для неё нет, поэтому
	// обработчики регистрируются до него: AddHandler после Start вернёт
	// ErrConsumerStarted.
	if err := consumer.AddHandler("orders", &orderHandler{}); err != nil {
		log.Fatal(err)
	}

	if err := consumer.Start(ctx); err != nil { // не блокирует
		log.Fatal(err)
	}

	defer consumer.Stop() //nolint:errcheck // пример из README

	// Консьюмер не перезапускается: после Stop и Start, и AddHandler вернут
	// ErrConsumerClosed, нужен новый.
	_ = []error{
		kafkax.ErrConsumerStarted,
		kafkax.ErrConsumerClosed,
		kafkax.ErrNoHandlers,
	}
}
