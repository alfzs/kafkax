package kafkax

import (
	"log/slog"
	"testing"

	"github.com/google/uuid"
)

// BenchmarkSendMessage_WarmPath воспроизводит ту часть SendMessage, что
// выполняется на каждый вызов до записи в messageChan: getOrCreateWorker. Это
// самый частый случай при установившейся нагрузке (воркер тенанта уже
// существует), в отличие от однократного создания воркера на тенанта.
func BenchmarkSendMessage_WarmPath(b *testing.B) {
	slog.SetDefault(slog.New(slog.DiscardHandler))

	p, err := NewKafkaProducer(b.Context(), testConfig())
	if err != nil {
		b.Skipf("librdkafka init failed: %v", err)
	}
	defer p.Close()

	tenantID := uuid.New()

	w, err := p.getOrCreateWorker(tenantID)
	if err != nil {
		b.Fatalf("getOrCreateWorker: %v", err)
	}

	w.inFlight.Add(-1)

	b.ReportAllocs()

	for b.Loop() {
		w, err := p.getOrCreateWorker(tenantID)
		if err != nil {
			b.Fatalf("getOrCreateWorker: %v", err)
		}

		w.inFlight.Add(-1)
	}
}
