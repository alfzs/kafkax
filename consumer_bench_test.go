package kafkax

import (
	"log/slog"
	"testing"
)

// BenchmarkProcessMessage_WarmPath воспроизводит ту часть processMessage, что
// выполняется на каждое сообщение до записи в messageChan: getOrCreateWorker.
// Это самый частый случай при установившейся нагрузке (воркер партиции уже
// существует), в отличие от однократного создания воркера на партицию.
func BenchmarkProcessMessage_WarmPath(b *testing.B) {
	slog.SetDefault(slog.New(slog.DiscardHandler))

	c, err := NewKafkaConsumer(testConfig())
	if err != nil {
		b.Skipf("librdkafka init failed: %v", err)
	}
	defer c.Stop()

	const partition = int32(0)

	w, err := c.getOrCreateWorker(testTopic, partition)
	if err != nil {
		b.Fatalf("getOrCreateWorker: %v", err)
	}

	w.inFlight.Add(-1)

	b.ReportAllocs()

	for b.Loop() {
		w, err := c.getOrCreateWorker(testTopic, partition)
		if err != nil {
			b.Fatalf("getOrCreateWorker: %v", err)
		}

		w.inFlight.Add(-1)
	}
}
