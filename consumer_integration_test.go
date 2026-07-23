//go:build integration

package kafkax

import (
	"testing"
	"time"
)

// TestKafkaConsumer_FullLifecycle проверяет полный жизненный цикл консьюмера:
// создание → регистрация обработчика → подписка → запуск → остановка. Помечен
// как integration: тест содержит реальные временные паузы против настоящего
// (недоступного) брокера, а не проверяет чистую логику — см.
// sprints/test-audit.md, находка 9.
func TestKafkaConsumer_FullLifecycle(t *testing.T) {
	t.Parallel()

	t.Log("шаг 1: создаём консьюмер")
	c := mustNewConsumer(t)

	t.Log("шаг 2: регистрируем обработчик для топика 'lifecycle-test'")

	handler := &mockHandler{}
	if err := c.AddHandler("lifecycle-test", handler); err != nil {
		t.Fatalf("AddHandler() завершился с ошибкой: %v", err)
	}

	t.Log("шаг 3: подписываемся на топики")

	if err := c.SubscribeAll(); err != nil {
		t.Fatalf("SubscribeAll() завершился с ошибкой: %v", err)
	}

	t.Log("шаг 4: запускаем consumer loop")

	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start() завершился с ошибкой: %v", err)
	}

	// Даём горутинам время запуститься; при недоступном брокере
	// consumer loop будет получать ошибки ReadMessage — это штатно.
	pause := 150 * time.Millisecond
	t.Logf("шаг 5: ожидаем %s для запуска горутин", pause)
	time.Sleep(pause)

	t.Log("шаг 6: останавливаем консьюмер")

	done := make(chan struct{})

	go func() {
		c.Stop()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Stop() завершился в пределах GracefulTimeout ✓")
	case <-time.After(testConfig().GracefulTimeout + time.Second):
		t.Fatalf("Stop() завис дольше GracefulTimeout=%s", testConfig().GracefulTimeout)
	}

	// ProcessMessage не должен был быть вызван — реального брокера нет.
	if calls := handler.callCount(); calls != 0 {
		t.Logf("информация: ProcessMessage был вызван %d раз (брокер оказался доступен)", calls)
	} else {
		t.Log("ProcessMessage не вызывался при недоступном брокере ✓")
	}
}
