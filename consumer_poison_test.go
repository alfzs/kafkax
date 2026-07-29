package kafkax

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestConsumerPoisonedPartitionResumesOnReassignment закрывает разрыв между
// обещанием документации и состоянием клиента.
//
// Отравленная партиция ставится на паузу через PauseFetchPartitions, а набор
// приостановленных партиций в franz-go принадлежит КЛИЕНТУ, а не назначению:
// ребаланс его не трогает, снять паузу могут только методы Resume*. Пока
// onPartitionsAssigned не звал ResumeFetchPartitions, партиция, вернувшаяся к
// тому же экземпляру, получала свежего воркера с poisoned=false, но выключенный
// фетч — и «сообщение приедет заново после ребаланса» было ложью для всех, кроме
// переезда на другой процесс.
func TestConsumerPoisonedPartitionResumesOnReassignment(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)

	cfg := testConfig(t, brokers...)
	// Эйджер-балансировщик вместо умолчания (cooperative-sticky) — не прихоть:
	// при кооперативном ребалансе партиция остаётся за прежним участником, и
	// колбэк назначения ему не приходит вовсе, так что сценарий «партиция
	// вернулась» на нём не воспроизводится. RoundRobin отзывает всё и раздаёт
	// заново, поэтому уход второго участника гарантированно оборачивается
	// назначением p0 первому.
	cfg.ExtraOpts = []kgo.Opt{kgo.Balancers(kgo.RoundRobinBalancer())}

	p := mustProducer(t, cfg)
	if err := p.SendMessage(t.Context(), PublishRequest{
		Topic: testTopic,
		Value: []byte("poison"),
	}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	failing := errors.New("обработчик падает всегда")

	handlerA := &mockHandler{returnErr: failing}
	consumerA := mustConsumer(t, cfg)
	mustAddHandler(t, consumerA, testTopic, handlerA)
	consStart(t, consumerA)

	waitFor(t, consWait, "первая доставка отравленного сообщения", func() bool {
		return handlerA.callCount() >= 1
	})

	// Второй участник запускает ребаланс. Кому именно достанется p0, тест не
	// загадывает: важно только, что после ухода второго она вернётся первому.
	handlerB := &mockHandler{returnErr: failing}
	consumerB := mustConsumer(t, cfg)
	mustAddHandler(t, consumerB, testTopic, handlerB)
	consStart(t, consumerB)

	waitFor(t, consWait, "ребаланс отдал партицию одному из участников", func() bool {
		return handlerB.callCount() >= 1 || handlerA.callCount() >= 2
	})

	if err := consumerB.Stop(); err != nil {
		t.Fatalf("Stop второго консьюмера: %v", err)
	}

	waitFor(t, consWait, "сообщение приехало первому консьюмеру заново", func() bool {
		return handlerA.callCount() >= 2
	})
}
