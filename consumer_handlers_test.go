package kafkax

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
)

// Тесты публикации карты обработчиков.
//
// Карта живёт под atomic.Pointer и переиздаётся копией на каждый AddHandler
// (RF-PERF-04). Проверяется не устройство, а два свойства, которыми снимок
// обязан обладать: путь сообщения никогда не видит полусобранную карту, и
// контракт «AddHandler только до Start» не поехал вместе со сменой
// синхронизации.

// TestConsumerHandlerSnapshotRejectedRegistrations проверяет, что отвергнутая
// регистрация оставляет снимок нетронутым.
//
// Это не формальность копирования: AddHandler теперь строит новую карту и
// публикует её. Публикация обязана быть последней операцией метода — иначе
// отказ по дубликату или по жизненному циклу подменял бы уже работающий
// снимок наполовину собранным.
func TestConsumerHandlerSnapshotRejectedRegistrations(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, unreachableBroker)

	t.Run("дубликат не подменяет обработчик", func(t *testing.T) {
		t.Parallel()

		c := mustConsumer(t, cfg)

		first := &mockHandler{}
		mustAddHandler(t, c, testTopic, first)

		if err := c.AddHandler(testTopic, &mockHandler{}); !errors.Is(err, ErrDuplicateHandler) {
			t.Fatalf("AddHandler (повтор) = %v, want ErrDuplicateHandler", err)
		}

		got, ok := c.handler(testTopic)
		if !ok {
			t.Fatal("после отвергнутого дубликата обработчик исчез из снимка")
		}

		if got != ConsumerHandler(first) {
			t.Fatal("отвергнутый дубликат всё-таки подменил обработчик в снимке")
		}
	})

	t.Run("после Start снимок не растёт", func(t *testing.T) {
		t.Parallel()

		// Топик, который пытаются зарегистрировать после точки невозврата.
		const late = "kafkax-snapshot-late-topic"

		c := mustConsumer(t, testConfig(t, newFakeCluster(t, 1, testTopic)...))
		mustAddHandler(t, c, testTopic, &mockHandler{})
		consStart(t, c)

		if err := c.AddHandler(late, &mockHandler{}); !errors.Is(err, ErrConsumerStarted) {
			t.Fatalf("AddHandler после Start = %v, want ErrConsumerStarted", err)
		}

		// Отказ обязан быть полным: топики уже уехали в kgo.ConsumeTopics, и
		// обработчик, попавший в снимок мимо подписки, не получил бы ни одного
		// сообщения, а метрики и логи показывали бы рабочую регистрацию.
		if _, ok := c.handler(late); ok {
			t.Fatal("обработчик, отвергнутый после Start, оказался в снимке")
		}

		if got := c.topics(); !slices.Equal(got, []string{testTopic}) {
			t.Fatalf("topics() = %v, want [%s]", got, testTopic)
		}
	})

	t.Run("после Stop снимок не растёт", func(t *testing.T) {
		t.Parallel()

		c := mustConsumer(t, cfg)

		if err := c.Stop(); err != nil {
			t.Fatalf("Stop до Start = %v, want nil", err)
		}

		if err := c.AddHandler(testTopic, &mockHandler{}); !errors.Is(err, ErrConsumerClosed) {
			t.Fatalf("AddHandler после Stop = %v, want ErrConsumerClosed", err)
		}

		if _, ok := c.handler(testTopic); ok {
			t.Fatal("обработчик, отвергнутый после Stop, оказался в снимке")
		}
	})
}

// TestConsumerHandlerSnapshotZeroValue проверяет, что консьюмер, собранный
// мимо конструктора, отвечает «обработчиков нет», а не паникует.
//
// Нулевой atomic.Pointer возвращает nil, и разыменование его на пути
// сообщения было бы паникой в горутине воркера — то есть падением процесса из
// самого горячего места пакета. Чтение из nil-карты законно, поэтому
// loadHandlers сводит нулевой указатель к пустому набору.
func TestConsumerHandlerSnapshotZeroValue(t *testing.T) {
	t.Parallel()

	var c KafkaConsumer

	if _, ok := c.handler(testTopic); ok {
		t.Fatal("нулевой консьюмер нашёл обработчик в пустом снимке")
	}

	if got := c.topics(); len(got) != 0 {
		t.Fatalf("topics() нулевого консьюмера = %v, want пусто", got)
	}
}

// TestConsumerHandlerSnapshotConcurrentPublish гоняет публикацию снимка против
// чтения с пути сообщения.
//
// Смысл теста — в детекторе гонок, а не в утверждениях: copy-on-write без
// atomic.Pointer или публикация до заполнения копии здесь и всплывают. Число
// читателей взято с запасом относительно писателей: в бою на один AddHandler
// приходятся миллионы вызовов handler.
func TestConsumerHandlerSnapshotConcurrentPublish(t *testing.T) {
	t.Parallel()

	const (
		writers = 8
		readers = 16
	)

	c := mustConsumer(t, testConfig(t, unreachableBroker))

	done := make(chan struct{})

	var wg sync.WaitGroup

	for range readers {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}

				// Результат не проверяется намеренно: пока писатели работают,
				// «нашёлся» и «не нашёлся» одинаково законны. Тест ловит
				// гонку, а не содержимое.
				c.handler(testTopic)
				c.topics()
			}
		})
	}

	for i := range writers {
		wg.Go(func() {
			topic := fmt.Sprintf("%s-%d", testTopic, i)
			if err := c.AddHandler(topic, &mockHandler{}); err != nil {
				t.Errorf("AddHandler(%q): %v", topic, err)
			}
		})
	}

	// Читатели останавливаются только после писателей: иначе часть публикаций
	// пришлась бы на пустой снимок и гонку было бы не на чём поймать.
	waitFor(t, consWait, "все обработчики опубликованы", func() bool {
		return len(c.topics()) == writers
	})

	close(done)
	wg.Wait()

	// Ни одна публикация не потеряна: copy-on-write под мьютексом обязан
	// сериализовать восемь параллельных вставок, а не оставить последнюю.
	if got := c.topics(); len(got) != writers {
		t.Fatalf("после %d параллельных AddHandler в снимке %d топиков: %v", writers, len(got), got)
	}
}
