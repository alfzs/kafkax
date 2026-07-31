package integration

// compression_test.go — кодеки сжатия против настоящего брокера.
//
// Зачем брокер. Маппинг Producer.CompressionType в kgo.CompressionCodec
// проверяется модульно (TestCompressionCodecMapping), но проверяется он
// сравнением с тем же самым значением из того же switch. Кодек, доехавший до
// брокера, виден только в брокере: он лежит в трёх младших битах атрибутов
// батча и приезжает обратно в Record.Attrs. До этой проверки набор гонял ровно
// один кодек — lz4 из DefaultConfig, — то есть четыре ветки switch не
// исполнялись против настоящего сервера ни разу.
//
// Круга «отправил — получил» здесь недостаточно, и это главное. Клиент
// распаковывает батч сам, по атрибутам батча, поэтому подмена одного кодека
// другим (или на «без сжатия») круг не ломает: отправили lz4 — получили lz4,
// отправили gzip вместо lz4 — получили gzip. Ассерт поэтому стоит на
// сохранённом кодеке, а круг — на том, что кодек не портит содержимое.

import (
	"context"
	"strings"
	"testing"

	"github.com/alfzs/kafkax/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Коды кодеков в атрибутах батча. Числа заданы протоколом Kafka, а не
// франц-го, и литералами они здесь намеренно: константа из библиотеки сверяла
// бы значение с ним же самим.
const (
	codecNone   uint8 = 0
	codecGzip   uint8 = 1
	codecSnappy uint8 = 2
	codecLZ4    uint8 = 3
	codecZstd   uint8 = 4
)

// compressiblePayload — тело, которое любой из кодеков заведомо сожмёт.
//
// Размер не декоративен. franz-go оставляет батч несжатым, если сжатие его не
// уменьшило (sink.go: len(compressed) < len(toCompress)), так что на коротком
// теле gzip и zstd дали бы кодек 0 при исправном маппинге, и тест краснел бы
// на здоровом коде.
var compressiblePayload = strings.Repeat("kafkax-compression-probe-", 512)

// TestCompressionCodecsRoundTrip проверяет все значения, которые принимает
// валидация Producer.CompressionType: круг «отправил — получил» и кодек,
// которым брокер сохранил батч.
func TestCompressionCodecsRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		codec string
		want  uint8
	}{
		{kafkax.CompressionNone, codecNone},
		{kafkax.CompressionGzip, codecGzip},
		{kafkax.CompressionSnappy, codecSnappy},
		{kafkax.CompressionLZ4, codecLZ4},
		{kafkax.CompressionZstd, codecZstd},
	}

	for _, tc := range cases {
		t.Run(tc.codec, func(t *testing.T) {
			t.Parallel()

			topic := newTopic(t, 1)
			cfg := testConfig(t)
			cfg.Producer.CompressionType = tc.codec

			producer := openProducer(t, cfg)
			if err := producer.SendMessage(t.Context(), kafkax.PublishRequest{
				Topic: topic,
				Value: []byte(compressiblePayload),
			}); err != nil {
				t.Fatalf("SendMessage с кодеком %s: %v", tc.codec, err)
			}

			handler := &collector{}
			startConsumer(t, cfg, topic, handler)

			await(t, "сообщение дошло до обработчика", func() bool {
				return handler.count() > 0
			})

			if got := handler.snapshot()[0]; got != compressiblePayload {
				t.Fatalf("кодек %s исказил тело: получено %d байт, отправлено %d",
					tc.codec, len(got), len(compressiblePayload))
			}

			if got := storedCodec(t, topic); got != tc.want {
				t.Fatalf("брокер сохранил батч под кодеком %d, want %d (%s)",
					got, tc.want, tc.codec)
			}
		})
	}
}

// storedCodec отдаёт кодек, под которым брокер сохранил первую запись темы.
//
// Чтение сырым клиентом мимо групп: Record.Attrs заполняется из атрибутов
// батча, пришедшего с брокера (franz-go, source.go), поэтому это утверждение о
// том, что в брокере лежит, а не о том, что тест отправлял.
func storedCodec(t *testing.T, topic string) uint8 {
	t.Helper()

	client := rawClient(t, brokers(t),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))

	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()

	for {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("чтение темы %s: %v", topic, err)
		}

		iter := fetches.RecordIter()
		if !iter.Done() {
			return iter.Next().Attrs.CompressionType()
		}
	}
}
