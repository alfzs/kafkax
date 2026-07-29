package kafkax

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
)

// prodBufferedHook считает записи, попавшие в буфер клиента.
//
// Хук вызывается внутри kgo.Produce, то есть уже после acquire в SendMessage.
// Это единственная наблюдаемая точка «отправка принята продюсером», и она
// позволяет строить гонку с Close на факте, а не на sleep.
type prodBufferedHook struct {
	n atomic.Int64
}

func (h *prodBufferedHook) OnProduceRecordBuffered(*kgo.Record) {
	h.n.Add(1)
}

var _ kgo.HookProduceRecordBuffered = (*prodBufferedHook)(nil)

// Close идемпотентен: второй вызов не паникует и возвращает nil.
//
// Обещание нужно вызывающему, который закрывает продюсер и из defer, и из
// обработчика сигнала: без него один из двух путей падал бы на закрытом
// клиенте внутри franz-go.
func TestProducerCloseIdempotent(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	p := mustProducer(t, testConfig(t, brokers...))

	if err := p.SendMessage(t.Context(), PublishRequest{Topic: testTopic, Value: []byte("v")}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("первый Close: %v", err)
	}

	// Документированное поведение повторного Close — предупреждение в лог и
	// nil, а не ошибка: закрытие уже закрытого не является отказом.
	for i := 2; i <= 3; i++ {
		if err := p.Close(); err != nil {
			t.Fatalf("Close #%d: %v, want nil", i, err)
		}
	}
}

// После Close новые отправки отбиваются, не доходя до клиента.
//
// ErrProducerClosed — единственная ошибка продюсера, означающая «сообщение
// точно не ушло»; если бы отправка проваливалась внутри franz-go, вызывающий
// получил бы неотличимое «не знаю, доехало ли».
func TestProducerSendMessageAfterClose(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	p := mustProducer(t, testConfig(t, brokers...))

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	tests := []struct {
		name string
		req  PublishRequest
	}{
		{name: "валидный запрос", req: PublishRequest{Topic: testTopic, Value: []byte("v")}},
		// Проверка закрытия стоит раньше валидации: закрытому продюсеру
		// нечего сообщать о качестве запроса, которого он не примет.
		{name: "пустой топик", req: PublishRequest{Value: []byte("v")}},
		{name: "битый заголовок", req: PublishRequest{
			Topic:   testTopic,
			Headers: Headers{{Key: ""}},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := p.SendMessage(t.Context(), tt.req)
			if !errors.Is(err, ErrProducerClosed) {
				t.Fatalf("SendMessage после Close = %v, want ErrProducerClosed", err)
			}
		})
	}
}

// Гонка Close и SendMessage: принятая отправка не должна оборваться.
//
// Это смысл связки RWMutex+WaitGroup в продюсере. Без неё между проверкой
// closing и inflight.Add успевает вклиниться Close, его Wait возвращается
// раньше уже принятой отправки, и клиент закрывается у неё под руками —
// вызывающий получает ErrProducerClosed на сообщении, которое продюсер принял.
//
// Синхронизация на факте, а не на времени: Close вызывается только после того,
// как все N записей попали в буфер клиента, то есть заведомо прошли acquire.
func TestProducerCloseWaitsForAcceptedSends(t *testing.T) {
	t.Parallel()

	const sends = 16

	brokers := newFakeCluster(t, 1, testTopic)

	hook := &prodBufferedHook{}
	cfg := testConfig(t, brokers...)
	cfg.ExtraOpts = []kgo.Opt{kgo.WithHooks(hook)}

	p, err := NewKafkaProducer(cfg)
	if err != nil {
		t.Fatalf("NewKafkaProducer: %v", err)
	}

	// Close идемпотентен и на повторе обязан вернуть nil, поэтому страховочное
	// закрытие здесь — тоже проверка, а не глушитель ошибки.
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("страховочный Close: %v, want nil", err)
		}
	})

	var (
		wg      sync.WaitGroup
		release = make(chan struct{})
		errs    = make([]error, sends)
	)

	for i := range sends {
		wg.Go(func() {
			<-release

			errs[i] = p.SendMessage(t.Context(), PublishRequest{
				Topic: testTopic,
				// Тело произвольно: тест считает записи, а не читает их.
				Value: fmt.Appendf(nil, "m-%02d", i),
			})
		})
	}

	close(release)

	waitFor(t, 10*time.Second, "все отправки приняты продюсером", func() bool {
		return hook.n.Load() == sends
	})

	closeErr := p.Close()

	wg.Wait()

	if closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}

	for i, err := range errs {
		if err != nil {
			t.Errorf("отправка %d, принятая до Close, оборвалась: %v", i, err)
		}
	}

	// Досланное при Close обязано быть читаемо: иначе «graceful» закрытие
	// молча теряет подтверждённые вызывающему записи.
	if got := len(prodFetchRecords(t, brokers, testTopic, sends)); got != sends {
		t.Fatalf("в топике %d записей, want %d", got, sends)
	}

	// Дверь закрыта в одну сторону: после возврата из Close продюсер больше
	// не принимает ничего.
	if err := p.SendMessage(t.Context(), PublishRequest{Topic: testTopic, Value: []byte("late")}); !errors.Is(err, ErrProducerClosed) {
		t.Fatalf("SendMessage после Close = %v, want ErrProducerClosed", err)
	}
}

// Close ровно посреди отправок: исход каждой обязан остаться однозначным.
//
// В отличие от теста выше барьера нет — Close попадает в произвольную точку
// каждой отправки. Проверяется инвариант, который и делает классификацию
// ошибок полезной: закрытие даёт либо nil (запись доехала и читается), либо
// ErrProducerClosed (запись НЕ доехала), но никогда ErrDeliveryFailed или
// ErrDeliveryTimeout — иначе вызывающий не знает, создаст ли повтор дубликат.
func TestProducerCloseRaceWithSends(t *testing.T) {
	t.Parallel()

	const sends = 32

	brokers := newFakeCluster(t, 1, testTopic)

	hook := &prodBufferedHook{}
	cfg := testConfig(t, brokers...)
	cfg.ExtraOpts = []kgo.Opt{kgo.WithHooks(hook)}

	p, err := NewKafkaProducer(cfg)
	if err != nil {
		t.Fatalf("NewKafkaProducer: %v", err)
	}

	// Close идемпотентен и на повторе обязан вернуть nil, поэтому страховочное
	// закрытие здесь — тоже проверка, а не глушитель ошибки.
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("страховочный Close: %v, want nil", err)
		}
	})

	var (
		wg      sync.WaitGroup
		release = make(chan struct{})
		errs    = make([]error, sends)
		values  = make([]string, sends)
	)

	for i := range sends {
		values[i] = fmt.Sprintf("m-%02d", i)

		wg.Go(func() {
			<-release

			errs[i] = p.SendMessage(t.Context(), PublishRequest{
				Topic: testTopic,
				Value: []byte(values[i]),
			})
		})
	}

	close(release)

	// Единственная синхронизация: ждём, пока хотя бы одна отправка попадёт в
	// буфер. Без неё Close мог бы опередить планировщик и отбить все 32 —
	// тест стал бы зелёным, ничего не проверив. Остальные отправки остаются в
	// произвольных точках относительно Close, ради чего тест и написан.
	waitFor(t, 10*time.Second, "хотя бы одна отправка принята продюсером", func() bool {
		return hook.n.Load() > 0
	})

	closeErr := p.Close()

	wg.Wait()

	if closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}

	delivered := make(map[string]bool)

	for i, err := range errs {
		switch {
		case err == nil:
			delivered[values[i]] = true
		case errors.Is(err, ErrProducerClosed):
		default:
			t.Fatalf("отправка %d при закрытии = %v, want nil или ErrProducerClosed", i, err)
		}
	}

	if len(delivered) == 0 {
		t.Fatal("ни одна отправка не прошла — гонка не воспроизвелась, тест ничего не проверил")
	}

	// Успех обязан быть подкреплён записью в топике, и наоборот: значение
	// отправки, вернувшей ErrProducerClosed, в топике оказаться не может.
	for _, rec := range prodFetchRecords(t, brokers, testTopic, len(delivered)) {
		value := string(rec.Value)
		if !delivered[value] {
			t.Errorf("в топике лежит %q, хотя эта отправка вернула ErrProducerClosed", value)

			continue
		}

		delete(delivered, value)
	}

	for value := range delivered {
		t.Errorf("отправка %q вернула nil, но записи в топике нет", value)
	}
}

// Успешная отправка увеличивает sent и пишет длительность со status=success.
//
// captureMetrics подменяет глобальный MeterProvider, поэтому t.Parallel здесь
// запрещён: соседний параллельный тест перемешал бы записи.
//
//nolint:paralleltest // подменяет глобальный MeterProvider
func TestProducerMetricsOnSuccess(t *testing.T) {
	const sends = 3

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, testTopic)
	p := mustProducer(t, testConfig(t, brokers...))

	topicAttr := attribute.String("topic", testTopic)

	for range sends {
		if err := p.SendMessage(t.Context(), PublishRequest{Topic: testTopic, Value: []byte("v")}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
	}

	if got := rec.sum("kafkax.producer.messages.sent", topicAttr); got != sends {
		t.Errorf("messages.sent = %d, want %d", got, sends)
	}

	if got := rec.sum("kafkax.producer.messages.failed", topicAttr); got != 0 {
		t.Errorf("messages.failed = %d, want 0", got)
	}

	obs := rec.observations("kafkax.producer.message.duration", topicAttr, attribute.String("status", statusSuccess))
	if len(obs) != sends {
		t.Fatalf("наблюдений message.duration(status=success) = %d, want %d", len(obs), sends)
	}

	// Единица — секунды, а не миллисекунды: при записи целыми миллисекундами
	// весь happy path локального брокера падал бы в нулевой бакет. Верхняя
	// граница ловит переход к миллисекундам — локальная отправка в kfake не
	// может занять секунду.
	for i, v := range obs {
		if v <= 0 || v > 1 {
			t.Errorf("message.duration[%d] = %v, ожидались секунды в (0, 1]", i, v)
		}
	}
}

// Провалившаяся отправка увеличивает failed и всё равно пишет длительность,
// но со status=error.
//
// Гистограмма только успешных отправок систематически занижает хвост: самые
// долгие вызовы — таймауты и отказы — из неё выпадают ровно тогда, когда
// длительность и интересна.
//
//nolint:paralleltest // подменяет глобальный MeterProvider
func TestProducerMetricsOnFailure(t *testing.T) {
	rec := captureMetrics(t)

	cluster := prodCluster(t, 1, testTopic)
	prodFailProduce(cluster, kerr.RecordListTooLarge.Code)

	cfg := testConfig(t, cluster.ListenAddrs()...)
	cfg.Producer.MaxRetries = 0

	p := mustProducer(t, cfg)

	topicAttr := attribute.String("topic", testTopic)

	if err := p.SendMessage(t.Context(), PublishRequest{Topic: testTopic, Value: []byte("v")}); err == nil {
		t.Fatal("SendMessage вернул nil, хотя брокер отверг запись")
	}

	if got := rec.sum("kafkax.producer.messages.failed", topicAttr); got != 1 {
		t.Errorf("messages.failed = %d, want 1", got)
	}

	if got := rec.sum("kafkax.producer.messages.sent", topicAttr); got != 0 {
		t.Errorf("messages.sent = %d, want 0", got)
	}

	obs := rec.observations("kafkax.producer.message.duration", topicAttr, attribute.String("status", statusError))
	if len(obs) != 1 {
		t.Fatalf("наблюдений message.duration(status=error) = %d, want 1", len(obs))
	}

	if obs[0] <= 0 || obs[0] > 5 {
		t.Errorf("message.duration = %v, ожидались секунды в (0, 5]", obs[0])
	}

	if got := len(rec.observations("kafkax.producer.message.duration", topicAttr,
		attribute.String("status", statusSuccess))); got != 0 {
		t.Errorf("наблюдений со status=success = %d, want 0", got)
	}
}

// Отбраковка на входе видна в метриках — но своим счётчиком и без topic.
//
// Терять её нельзя: приложение, которое шлёт один невалидный запрос за другим,
// иначе выглядит в мониторинге идеально здоровым — сообщения не доезжают, а все
// счётчики нули. Но и класть её в messages.failed с атрибутом topic нельзя:
// значение приходит снаружи и ничем не ограничено, так что отбракованный
// запрос рождал бы серию на каждое уникальное значение — прямой путь к взрыву
// кардинальности ровно теми запросами, которые пакет отверг не глядя. Брокер
// здесь не нужен: до сети вызов не доходит.
//
//nolint:paralleltest // подменяет глобальный MeterProvider
func TestProducerMetricsOnValidationFailure(t *testing.T) {
	rec := captureMetrics(t)

	p := mustProducer(t, testConfig(t))

	topicAttr := attribute.String("topic", testTopic)

	err := p.SendMessage(t.Context(), PublishRequest{
		Topic:   testTopic,
		Value:   []byte("v"),
		Headers: Headers{{Key: "", Value: []byte("v")}},
	})
	if !errors.Is(err, ErrEmptyHeaderKey) {
		t.Fatalf("SendMessage = %v, want ErrEmptyHeaderKey", err)
	}

	if got := rec.sum("kafkax.producer.messages.rejected",
		attribute.String("reason", rejectInvalidHeaders)); got != 1 {
		t.Errorf("messages.rejected{reason=invalid_headers} = %d, want 1", got)
	}

	if err := p.SendMessage(t.Context(), PublishRequest{Value: []byte("v")}); !errors.Is(err, ErrEmptyTopic) {
		t.Fatalf("SendMessage(topic=\"\") = %v, want ErrEmptyTopic", err)
	}

	if got := rec.sum("kafkax.producer.messages.rejected",
		attribute.String("reason", rejectEmptyTopic)); got != 1 {
		t.Errorf("messages.rejected{reason=empty_topic} = %d, want 1", got)
	}

	// Ни один атрибут topic не заведён — ни валидный, ни пустой. Именно это
	// отличает исправленное поведение от прежнего, где отбраковка шла общим
	// путём исхода и тащила req.Topic в три инструмента сразу.
	for _, attr := range []attribute.KeyValue{topicAttr, attribute.String("topic", "")} {
		if got := rec.sum("kafkax.producer.messages.failed", attr); got != 0 {
			t.Errorf("messages.failed{%v} = %d, want 0", attr, got)
		}

		if got := rec.sum("kafkax.producer.messages.sent", attr); got != 0 {
			t.Errorf("messages.sent{%v} = %d, want 0", attr, got)
		}

		for _, status := range []string{statusError, statusSuccess} {
			if got := len(rec.observations("kafkax.producer.message.duration", attr,
				attribute.String("status", status))); got != 0 {
				t.Errorf("наблюдений message.duration{%v,status=%s} = %d, want 0", attr, status, got)
			}
		}
	}
}

// Таймаут доставки тоже попадает в метрики — и как failed, и в гистограмму.
//
// Отдельно от отказа брокера: таймаут проходит другим путём внутри franz-go
// (запись не доходит до сети), и легко потерять именно его.
//
//nolint:paralleltest // подменяет глобальный MeterProvider
func TestProducerMetricsOnDeliveryTimeout(t *testing.T) {
	rec := captureMetrics(t)

	cfg := testConfig(t)
	// 1s — минимум RecordDeliveryTimeout в franz-go.
	cfg.Producer.MessageTimeout = time.Second

	p := mustProducer(t, cfg)

	topicAttr := attribute.String("topic", testTopic)

	err := p.SendMessage(t.Context(), PublishRequest{Topic: testTopic, Value: []byte("v")})
	if !errors.Is(err, ErrDeliveryTimeout) {
		t.Fatalf("SendMessage = %v, want ErrDeliveryTimeout", err)
	}

	if got := rec.sum("kafkax.producer.messages.failed", topicAttr); got != 1 {
		t.Errorf("messages.failed = %d, want 1", got)
	}

	obs := rec.observations("kafkax.producer.message.duration", topicAttr, attribute.String("status", statusError))
	if len(obs) != 1 {
		t.Fatalf("наблюдений message.duration(status=error) = %d, want 1", len(obs))
	}

	// Длительность должна быть соизмерима с бюджетом (1s), а не с нулём:
	// именно так проверяется, что гистограмма измеряет весь вызов, а не
	// только его успешную часть.
	if obs[0] < 0.5 || obs[0] > 5 {
		t.Errorf("message.duration = %v, ожидалось около 1 (секунды)", obs[0])
	}
}

// Close, вызванный во время отправки на недоступный брокер, укладывается в
// GracefulTimeout и не виснет на inflight-вызове.
//
// Бюджет здесь — защита от вызывающего, который передал контекст, живущий
// дольше всего shutdown'а; без awaitInflight с дедлайном закрытие приложения
// зависело бы от чужого контекста.
//
//nolint:paralleltest // измеряет длительность Close относительно GracefulTimeout
func TestProducerCloseBoundedByGracefulTimeout(t *testing.T) {
	cfg := testConfig(t)
	// MessageTimeout заведомо больше GracefulTimeout: закрытие обязано
	// вернуться по своему бюджету, а не дождаться отправки.
	cfg.Producer.MessageTimeout = 30 * time.Second
	cfg.GracefulTimeout = 300 * time.Millisecond
	cfg.Producer.FlushTimeout = 300 * time.Millisecond

	p, err := NewKafkaProducer(cfg)
	if err != nil {
		t.Fatalf("NewKafkaProducer: %v", err)
	}

	sendDone := make(chan error, 1)
	started := make(chan struct{})

	go func() {
		close(started)

		sendDone <- p.SendMessage(context.Background(), PublishRequest{Topic: testTopic, Value: []byte("v")})
	}()

	<-started

	// Продюсер закрывается, пока отправка ещё висит: без ограничения
	// awaitInflight Close ждал бы все 30 секунд MessageTimeout.
	closeStart := time.Now()

	// Ошибка здесь ожидаема и не проверяется: брокер недоступен, flush не
	// успевает за свой бюджет. Тест про длительность Close, а не про его исход.
	if err := p.Close(); err != nil {
		t.Logf("Close (ошибка ожидаема на недоступном брокере): %v", err)
	}

	if elapsed := time.Since(closeStart); elapsed > 5*time.Second {
		t.Fatalf("Close занял %s при GracefulTimeout=%s", elapsed, cfg.GracefulTimeout)
	}

	select {
	case err := <-sendDone:
		// Закрытие клиента обрывает висящую отправку; ей полагается ошибка
		// закрытия, а не успех.
		if err == nil {
			t.Fatal("SendMessage вернул nil, хотя продюсер закрылся на висящей отправке")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SendMessage не вернулся после Close")
	}
}
