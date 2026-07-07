package kafkax

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain проверяет отсутствие утечек горутин после завершения всех тестов
// пакета. Обе намеренно неприсоединяемые горутины (наблюдатель за ctx в
// NewKafkaProducer, см. producer.go:224, и хелпер wg.Wait();close(done) в
// Close()/Stop()) завершаются к моменту возврата из Close()/Stop(), которые
// вызываются через t.Cleanup в mustNewProducer/mustNewConsumer — поэтому к
// моменту вызова goleak ни одна из них не должна быть активна.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
