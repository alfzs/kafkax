package kafkax

import (
	"context"
	"testing"
	"time"
)

func TestSmokeRoundTrip(t *testing.T) {
	// Тест держит собственный kfake-кластер и не трогает глобальные провайдеры
	// OTel, поэтому соседей по параллельному прогону ему испортить нечем.
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	cfg := testConfig(t, brokers...)

	p := mustProducer(t, cfg)
	if err := p.SendMessage(t.Context(), PublishRequest{
		Topic: testTopic,
		Key:   []byte("k"),
		Value: []byte("v"),
	}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	h := &mockHandler{}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitFor(t, 15*time.Second, "сообщение доехало до обработчика", func() bool {
		return h.callCount() == 1
	})

	if got := string(h.messages()[0].Value); got != "v" {
		t.Fatalf("value = %q, want %q", got, "v")
	}

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
