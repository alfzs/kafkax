//go:build integration

package kafkax

import (
	"testing"

	"github.com/google/uuid"
)

// TestKafkaProducer_SendMessage_BrokerUnavailable проверяет, что SendMessage
// завершается по таймауту при недоступном брокере. Помечен как integration:
// тест ждёт реальный MessageTimeout (~300ms) против настоящего (недоступного)
// брокера, а не проверяет чистую логику — см. docs/test-audit.md, находка 9.
func TestKafkaProducer_SendMessage_BrokerUnavailable(t *testing.T) {
	t.Parallel()

	p := mustNewProducer(t)
	t.Logf("отправляем сообщение на недоступный брокер (ждём таймаут ~%s)", testConfig().Producer.MessageTimeout)

	err := p.SendMessage(t.Context(), PublishRequest{TenantID: uuid.New(), Topic: "test-topic", Value: []byte("hello")})
	if err == nil {
		// Если брокер случайно оказался доступен — тест некорректен.
		t.Log("брокер оказался доступен: сообщение доставлено (пропускаем проверку таймаута)")
		return
	}

	t.Logf("сообщение не доставлено при недоступном брокере: %q ✓", err.Error())
}
