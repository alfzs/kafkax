package integration

// acks_test.go — Producer.RequiredAcks против настоящего брокера.
//
// Зачем брокер. acks — единственная настройка продюсера, у которой нет
// наблюдаемого следа ни в записи, ни в ответе клиента: она меняет только то,
// чего брокер ждёт перед ответом, и на исправном одиночном кластере все три
// значения дают одинаковый зелёный круг. Набор гонял поэтому только
// умолчание -1, а ветки acks(0) и acks(1) против сервера не исполнялись.
//
// Как отличаются три значения. Различить их можно лишь тем, ЧТО брокер
// откажется подтвердить, поэтому здесь два независимых зонда:
//
//   - тема с min.insync.replicas=2 при RF=1. Брокер сверяет ISR только при
//     acks=-1, поэтому acks=0 и acks=1 на ней пишут, а acks=-1 отказывает
//     с NOT_ENOUGH_REPLICAS. Зонд отделяет 0 и 1 от -1.
//   - тема с max.message.bytes меньше записи. Брокер отвергает её ответом,
//     а ответа при acks=0 нет вовсе — отказ туда не доезжает и запись просто
//     теряется. Зонд отделяет 0 от 1 и -1.
//
// Вместе они закрепляют каждое из трёх значений: подмена acks(0) на любое
// другое краснеет на втором зонде, подмена acks(1) на AllISRAcks — на первом,
// на NoAck — на втором.
//
// Круг «отправил — получил» при acks=0 проверяется чтением темы: подтверждения
// нет по определению, и возврат SendMessage о доставке не говорит ничего.

import (
	"strings"
	"testing"

	"github.com/alfzs/kafkax/v2"
)

// oversizedValue заведомо не влезает в maxMessageBytes. Сжатие у продюсера в
// этих сценариях выключено (см. acksConfig): max.message.bytes брокер сверяет
// с размером батча ПОСЛЕ сжатия, и сжимаемое тело проехало бы лимит насквозь.
var oversizedValue = strings.Repeat("x", 8192)

// maxMessageBytes — потолок темы-зонда, заведомо ниже oversizedValue.
const maxMessageBytes = "1024"

// TestAcksZeroDoesNotWaitForBroker проверяет acks=0: доставка идёт, а отказ
// брокера до продюсера не доезжает.
func TestAcksZeroDoesNotWaitForBroker(t *testing.T) {
	t.Parallel()

	cfg := acksConfig(t, 0)

	t.Run("круг без подтверждения", func(t *testing.T) {
		t.Parallel()

		// Тема требует двух реплик в ISR, а реплика одна: acks=-1 здесь
		// невыполним, а acks=0 брокера ни о чём не спрашивает.
		topic := newTopicWith(t, 1, minISRConfig())
		producer := openProducer(t, roleConfig(t, cfg))

		publishValues(t, producer, topic, "a0-first", "a0-second")

		// Ассерт на содержимом темы, а не на возврате SendMessage: при acks=0
		// он вернул бы nil и для записи, которой брокер не принял.
		await(t, "обе записи легли в тему", func() bool {
			return len(readTopic(t, brokers(t), topic)) == 2
		})
	})

	t.Run("отказ брокера не виден отправителю", func(t *testing.T) {
		t.Parallel()

		topic := newTopicWith(t, 1, oversizeConfig())
		producer := openProducer(t, roleConfig(t, cfg))

		if err := producer.SendMessage(t.Context(), kafkax.PublishRequest{
			Topic: topic,
			Value: []byte(oversizedValue),
		}); err != nil {
			t.Fatalf("SendMessage при acks=0 обязан вернуть nil, вернул: %v", err)
		}

		// Запись при этом до темы не доехала — ровно то, чем acks=0 и платит
		// за скорость. Проверяется, чтобы «ошибки нет» не читалось как
		// «всё хорошо»: без этого ассерта тест зеленел бы и на брокере,
		// который запись принял.
		if got := readTopic(t, brokers(t), topic); len(got) != 0 {
			t.Fatalf("брокер принял запись сверх max.message.bytes: %d шт.", len(got))
		}
	})
}

// TestAcksLeaderWaitsForLeaderOnly проверяет acks=1: подтверждает лидер, ISR не
// сверяется, отказ брокера доезжает до отправителя.
func TestAcksLeaderWaitsForLeaderOnly(t *testing.T) {
	t.Parallel()

	cfg := acksConfig(t, 1)

	t.Run("круг через лидера", func(t *testing.T) {
		t.Parallel()

		// Та же тема с недостижимым min.insync.replicas: acks=1 обязан
		// пройти по ней, acks=-1 — нет.
		topic := newTopicWith(t, 1, minISRConfig())
		roundTrip := roleConfig(t, cfg)
		producer := openProducer(t, roundTrip)

		publishValues(t, producer, topic, "a1-first", "a1-second")

		handler := &collector{}
		startConsumer(t, roundTrip, topic, handler)

		await(t, "обе записи дошли до обработчика", func() bool {
			return handler.has("a1-first") && handler.has("a1-second")
		})
	})

	t.Run("отказ брокера доезжает до отправителя", func(t *testing.T) {
		t.Parallel()

		topic := newTopicWith(t, 1, oversizeConfig())
		producer := openProducer(t, roleConfig(t, cfg))

		err := producer.SendMessage(t.Context(), kafkax.PublishRequest{
			Topic: topic,
			Value: []byte(oversizedValue),
		})
		if err == nil {
			t.Fatal("SendMessage при acks=1 обязан вернуть отказ брокера, вернул nil")
		}
	})
}

// acksConfig — конфигурация продюсера с заданным acks.
//
// EnableIdempotence=false обязателен: Validate отвергает acks≠-1 вместе с
// идемпотентностью, потому что дедупликация на брокере без подтверждения
// последовательности смысла не имеет.
func acksConfig(t *testing.T, acks int) kafkax.Config {
	t.Helper()

	cfg := testConfig(t)
	cfg.Producer.RequiredAcks = acks
	cfg.Producer.EnableIdempotence = false
	// Сжатие выключено ради зонда max.message.bytes, см. oversizedValue.
	cfg.Producer.CompressionType = kafkax.CompressionNone

	return cfg
}

// roleConfig подставляет группу подтеста: testConfig выводит её из имени
// теста, а конфигурация здесь собирается уровнем выше, в родительском.
func roleConfig(t *testing.T, cfg kafkax.Config) kafkax.Config {
	t.Helper()

	cfg.Consumer.Group = newGroup(t)

	return cfg
}

// minISRConfig — тема, на которой acks=-1 невыполним: две реплики в ISR при
// одной существующей.
func minISRConfig() map[string]*string {
	return map[string]*string{"min.insync.replicas": new("2")}
}

// oversizeConfig — тема, которая отвергает запись размером с oversizedValue.
func oversizeConfig() map[string]*string {
	return map[string]*string{"max.message.bytes": new(maxMessageBytes)}
}
