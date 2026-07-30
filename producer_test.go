package kafkax

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// prodHeaderRetry — произвольное прикладное имя заголовка. Вынесено в
// константу, потому что тест дублирует его намеренно: на повторе одного и того
// же имени проверяется, что Headers остаются списком, а не мапой.
const prodHeaderRetry = "x-retry"

// prodFetchRecords вычитывает из топика ровно want записей сырым клиентом
// franz-go.
//
// Читает не KafkaConsumer: тест продюсера, проверяющий себя через консьюмер
// того же пакета, зелёный и при симметричной ошибке в обоих — например если бы
// продюсер и консьюмер одинаково перепутали местами ключ и значение.
// Группы нет намеренно: координация группы добавила бы к тесту продюсера
// ребаланс, который к нему отношения не имеет.
func prodFetchRecords(t *testing.T, brokers []string, topic string, want int) []*kgo.Record {
	t.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}

	defer cl.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	out := make([]*kgo.Record, 0, want)

	for len(out) < want {
		fetches := cl.PollRecords(ctx, want-len(out))
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("вычитывание %q: %v (получено %d из %d)", topic, errs, len(out), want)
		}

		fetches.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}

	return out
}

// prodHeader возвращает значение первого заголовка записи с данным именем.
func prodHeader(rec *kgo.Record, key string) ([]byte, bool) {
	for _, h := range rec.Headers {
		if h.Key == key {
			return h.Value, true
		}
	}

	return nil, false
}

// prodTracerProvider отдаёт один и тот же записывающий трейсер на любой scope.
type prodTracerProvider struct {
	tracenoop.TracerProvider

	tracer *recordingTracer
}

func (p prodTracerProvider) Tracer(_ string, _ ...trace.TracerOption) trace.Tracer {
	return p.tracer
}

// prodFailProduce заставляет kfake отвечать на КАЖДЫЙ Produce указанным кодом
// ошибки, не трогая остальные запросы (метаданные, ApiVersions).
//
// Ответ собирается вручную, а не выбирается из готовых: kfake умеет только
// успешный produce, а нам нужен путь «брокер принял запрос и отверг запись» —
// единственный, на котором SendMessage обязан отдать *kerr.Error под
// ErrDeliveryFailed.
func prodFailProduce(cluster *kfake.Cluster, code int16) {
	cluster.ControlKey(kmsg.Produce.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		preq, ok := req.(*kmsg.ProduceRequest)
		if !ok {
			return nil, nil, false
		}

		// Без KeepControl функция снялась бы после первого же ответа, и
		// повторная отправка в том же тесте прошла бы успешно.
		cluster.KeepControl()

		presp, ok := preq.ResponseKind().(*kmsg.ProduceResponse)
		if !ok {
			return nil, nil, false
		}

		for _, reqTopic := range preq.Topics {
			respTopic := kmsg.NewProduceResponseTopic()
			respTopic.Topic = reqTopic.Topic
			respTopic.TopicID = reqTopic.TopicID

			for _, reqPart := range reqTopic.Partitions {
				respPart := kmsg.NewProduceResponseTopicPartition()
				respPart.Partition = reqPart.Partition
				respPart.ErrorCode = code
				respTopic.Partitions = append(respTopic.Partitions, respPart)
			}

			presp.Topics = append(presp.Topics, respTopic)
		}

		return presp, nil, true
	})
}

// prodCluster поднимает брокер kfake напрямую, когда тесту нужен сам *Cluster
// (для Control), а не только адреса, которые отдаёт newFakeCluster.
func prodCluster(t *testing.T, partitions int32, topics ...string) *kfake.Cluster {
	t.Helper()

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(partitions, topics...),
	)
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}

	t.Cleanup(cluster.Close)

	return cluster
}

// Полный путь записи: то, что положили в PublishRequest, должно приехать в
// топик побайтово и в том же порядке заголовков.
//
// Проверяется именно содержимое, а не «ошибки не было»: перепутанные местами
// Key и Value, потерянные заголовки или отправка не в тот топик — ровно те
// ошибки, при которых SendMessage возвращает nil.
func TestProducerSendMessageDeliversRecord(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	p := mustProducer(t, testConfig(t, brokers...))

	req := PublishRequest{
		Topic: testTopic,
		Key:   []byte("order-42"),
		Value: []byte(`{"amount":100}`),
		Headers: Headers{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: prodHeaderRetry, Value: []byte("0")},
			// Дубликат имени: протокол Kafka его допускает, и Headers
			// документированы как список, а не как мапа.
			{Key: prodHeaderRetry, Value: []byte("1")},
		},
	}

	if err := p.SendMessage(t.Context(), req); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	recs := prodFetchRecords(t, brokers, testTopic, 1)
	rec := recs[0]

	if rec.Topic != testTopic {
		t.Errorf("topic = %q, want %q", rec.Topic, testTopic)
	}

	if !bytes.Equal(rec.Key, req.Key) {
		t.Errorf("key = %q, want %q", rec.Key, req.Key)
	}

	if !bytes.Equal(rec.Value, req.Value) {
		t.Errorf("value = %q, want %q", rec.Value, req.Value)
	}

	for _, want := range []Header{
		{Key: "content-type", Value: []byte("application/json")},
		{Key: prodHeaderRetry, Value: []byte("0")},
	} {
		got, ok := prodHeader(rec, want.Key)
		if !ok {
			t.Errorf("заголовок %q не доехал", want.Key)

			continue
		}

		if !bytes.Equal(got, want.Value) {
			t.Errorf("заголовок %q = %q, want %q", want.Key, got, want.Value)
		}
	}

	var retryCount int

	for _, h := range rec.Headers {
		if h.Key == prodHeaderRetry {
			retryCount++
		}
	}

	if retryCount != 2 {
		t.Errorf("заголовков %s = %d, want 2 (дубликаты ключей не должны схлопываться)", prodHeaderRetry, retryCount)
	}
}

// Пустой топик отвергается на границе API, до обращения к брокеру.
//
// Без этой проверки запись ушла бы в defaultProduceTopic франц-го (пустой), и
// ошибка всплыла бы уже внутри клиента — с текстом, по которому не видно, что
// виноват вызывающий.
func TestProducerSendMessageEmptyTopic(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	p := mustProducer(t, testConfig(t, brokers...))

	err := p.SendMessage(t.Context(), PublishRequest{Value: []byte("v")})
	if !errors.Is(err, ErrEmptyTopic) {
		t.Fatalf("SendMessage(topic=\"\") = %v, want ErrEmptyTopic", err)
	}

	// Отказ должен быть чистым: продюсер остаётся рабочим, и в топике ровно
	// одна запись — та, которую отправили после отказа.
	if err := p.SendMessage(t.Context(), PublishRequest{Topic: testTopic, Value: []byte("marker")}); err != nil {
		t.Fatalf("SendMessage после отказа: %v", err)
	}

	recs := prodFetchRecords(t, brokers, testTopic, 1)
	if got := string(recs[0].Value); got != "marker" {
		t.Fatalf("первая запись топика = %q, want %q", got, "marker")
	}
}

// Заголовки валидируются до отправки, а не после.
//
// Пустое имя ломает чтение у произвольного клиента, а traceparent/tracestate/
// baggage kotel молча перезапишет своими значениями — в обоих случаях запись
// не должна доехать до брокера вовсе.
func TestProducerSendMessageInvalidHeaders(t *testing.T) {
	t.Parallel()

	brokers := newFakeCluster(t, 1, testTopic)
	p := mustProducer(t, testConfig(t, brokers...))

	tests := []struct {
		name    string
		headers Headers
		want    error
	}{
		{
			name:    "пустое имя",
			headers: Headers{{Key: "", Value: []byte("v")}},
			want:    ErrEmptyHeaderKey,
		},
		{
			name:    "пустое имя после валидного",
			headers: Headers{{Key: "ok", Value: []byte("v")}, {Key: ""}},
			want:    ErrEmptyHeaderKey,
		},
		{
			name:    headerKeyTraceparent,
			headers: Headers{{Key: headerKeyTraceparent, Value: []byte("00-x-y-01")}},
			want:    ErrReservedHeaderKey,
		},
		{
			name:    headerKeyTracestate,
			headers: Headers{{Key: headerKeyTracestate, Value: []byte("a=b")}},
			want:    ErrReservedHeaderKey,
		},
		{
			name:    headerKeyBaggage,
			headers: Headers{{Key: headerKeyBaggage, Value: []byte("k=v")}},
			want:    ErrReservedHeaderKey,
		},
	}

	// Подтесты идут последовательно: они делят один продюсер и один топик, а
	// маркер после цикла обязан оказаться первой записью в нём — параллельные
	// подтесты вернули бы управление до отправки маркера.
	//nolint:paralleltest // подтесты делят продюсер и порядок записи в топик
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := p.SendMessage(t.Context(), PublishRequest{
				Topic:   testTopic,
				Value:   []byte("не должно доехать"),
				Headers: tt.headers,
			})
			if !errors.Is(err, tt.want) {
				t.Fatalf("SendMessage = %v, want %v", err, tt.want)
			}
		})
	}

	// Маркер отправляется после всех отказов и обязан оказаться первой
	// записью топика: любая отвергнутая запись всплыла бы перед ним.
	if err := p.SendMessage(t.Context(), PublishRequest{Topic: testTopic, Value: []byte("marker")}); err != nil {
		t.Fatalf("SendMessage(marker): %v", err)
	}

	recs := prodFetchRecords(t, brokers, testTopic, 1)
	if got := string(recs[0].Value); got != "marker" {
		t.Fatalf("первая запись топика = %q, want %q — отвергнутая запись всё-таки ушла", got, "marker")
	}
}

// nil и пустые Key/Value — валидные значения, а не «забытые поля».
//
// nil-ключ означает распределение по кругу, nil-значение — tombstone, который
// compacted-топик трактует как удаление ключа. Проверка защищает от соблазна
// «заодно» отвергнуть их вместе с пустым топиком.
//
//nolint:paralleltest // подтесты последовательны: порядок записей в топике сверяется с порядком таблицы
func TestProducerSendMessageNilAndEmptyKeyValue(t *testing.T) {
	t.Parallel()

	const topic = "kafkax-test-tombstone"

	brokers := newFakeCluster(t, 1, topic)
	p := mustProducer(t, testConfig(t, brokers...))

	tests := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{name: "nil ключ и nil значение"},
		{name: "nil ключ, есть значение", value: []byte("v")},
		{name: "есть ключ, nil значение (tombstone)", key: []byte("k")},
		{name: "пустые непустые слайсы", key: []byte{}, value: []byte{}},
	}

	// Подтесты идут последовательно: ниже записи сверяются по индексу с
	// порядком таблицы, а параллельные подтесты его не сохраняют.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := p.SendMessage(t.Context(), PublishRequest{
				Topic: topic,
				Key:   tt.key,
				Value: tt.value,
			}); err != nil {
				t.Fatalf("SendMessage: %v", err)
			}
		})
	}

	recs := prodFetchRecords(t, brokers, topic, len(tests))

	// Kafka не различает nil и пустой слайс: наружу и то и другое приезжает
	// как отсутствие данных, поэтому сравнивается длина, а не сам слайс.
	for i, want := range tests {
		if got, wantLen := len(recs[i].Value), len(want.value); got != wantLen {
			t.Errorf("запись %d (%s): len(value) = %d, want %d", i, want.name, got, wantLen)
		}
	}
}

// Контекст, отменённый до вызова, обязан отменить и отправку.
//
// Гарантия для вызывающего: если его запрос уже отменён, продюсер не тратит
// на него MessageTimeout и возвращает именно context.Canceled, по которому
// видно, что виноват не брокер.
//
//nolint:paralleltest // измеряет длительность возврата: параллельная нагрузка размывает границу
func TestProducerSendMessageCanceledContext(t *testing.T) {
	brokers := newFakeCluster(t, 1, testTopic)
	p := mustProducer(t, testConfig(t, brokers...))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	start := time.Now()

	err := p.SendMessage(ctx, PublishRequest{Topic: testTopic, Value: []byte("v")})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SendMessage(отменённый ctx) = %v, want context.Canceled", err)
	}

	// Отмена не должна превращаться в ожидание MessageTimeout (3s в
	// testConfig): бюджет тратится только на живой контекст.
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("возврат занял %s — отменённый контекст не отработал сразу", elapsed)
	}

	if errors.Is(err, ErrDeliveryTimeout) {
		t.Errorf("отмена не должна выдаваться за таймаут доставки: %v", err)
	}
}

// Недоступный брокер: весь путь отправки ограничен одним MessageTimeout.
//
// Это главное обещание продюсера: настройка задаёт общий бюджет на весь путь
// сообщения, а не таймаут отдельного этапа, поэтому худший случай не
// превышает её значения.
//
//nolint:paralleltest // измеряет, что весь путь укладывается в один MessageTimeout
func TestProducerSendMessageDeliveryTimeout(t *testing.T) {
	cfg := testConfig(t)
	// 1s — минимум, который принимает franz-go для RecordDeliveryTimeout;
	// меньше отвергает конструктор клиента.
	cfg.Producer.MessageTimeout = time.Second

	p := mustProducer(t, cfg)

	start := time.Now()

	err := p.SendMessage(t.Context(), PublishRequest{Topic: testTopic, Value: []byte("v")})
	if !errors.Is(err, ErrDeliveryTimeout) {
		t.Fatalf("SendMessage(недоступный брокер) = %v, want ErrDeliveryTimeout", err)
	}

	// Верхняя граница щедрая, нижняя — точная: смысл проверки в том, что
	// бюджет один, а не в скорости машины.
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("возврат занял %s при MessageTimeout=%s — бюджет не один", elapsed, cfg.Producer.MessageTimeout)
	}

	// Таймаут — отдельный класс от «продюсер закрыт»: он означает «запись
	// могла доехать», и вызывающий не должен принять его за «точно не ушла».
	if errors.Is(err, ErrProducerClosed) {
		t.Errorf("таймаут не должен выдаваться за ErrProducerClosed: %v", err)
	}

	// Сентинела мало: под ним обязана лежать причина. Их две — исчерпанный
	// бюджет вызова и неспособность клиента дослать запись, — и какая из них
	// сработает на недоступном брокере, решает гонка двух таймеров. Проверяется
	// поэтому не конкретная, а сам факт: причина не потеряна.
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, kgo.ErrRecordTimeout) {
		t.Errorf("ErrDeliveryTimeout пришёл без причины: %v", err)
	}
}

// Брокер отверг запись: ошибка обёрнута дважды.
//
// errors.Is доводит до sentinel'а пакета, errors.As — до *DeliveryError с кодом
// брокера. Потеря любой из двух обёрток лишает вызывающего единственного
// способа решить, безопасен ли повтор: RECORD_LIST_TOO_LARGE повторять
// бессмысленно, NOT_ENOUGH_REPLICAS — наоборот.
func TestProducerSendMessageBrokerError(t *testing.T) {
	t.Parallel()

	cluster := prodCluster(t, 1, testTopic)

	// Код неповторяемый (Retriable=false), иначе franz-go ретраил бы до
	// исчерпания MessageTimeout и тест проверял бы таймаут, а не отказ.
	prodFailProduce(cluster, kerr.RecordListTooLarge.Code)

	cfg := testConfig(t, cluster.ListenAddrs()...)
	cfg.Producer.MaxRetries = 0

	p := mustProducer(t, cfg)

	err := p.SendMessage(t.Context(), PublishRequest{Topic: testTopic, Value: []byte("v")})
	if err == nil {
		t.Fatal("SendMessage вернул nil, хотя брокер отверг запись")
	}

	if !errors.Is(err, ErrDeliveryFailed) {
		t.Errorf("errors.Is(err, ErrDeliveryFailed) = false; err = %v", err)
	}

	var delivery *DeliveryError
	if !errors.As(err, &delivery) {
		t.Fatalf("errors.As(err, *DeliveryError) = false; err = %v", err)
	}

	if delivery.Code != kerr.RecordListTooLarge.Code {
		t.Errorf("код брокера = %d (%s), want %d",
			delivery.Code, delivery.Name, kerr.RecordListTooLarge.Code)
	}

	if delivery.Topic != testTopic {
		t.Errorf("DeliveryError.Topic = %q, want %q", delivery.Topic, testTopic)
	}

	// Код неповторяемый — именно поэтому он и выбран для теста; если бы пакет
	// подставлял Retriable наугад, потребитель ушёл бы в бесконечный ретрай.
	if delivery.Retriable {
		t.Errorf("Retriable = true для %s", delivery.Name)
	}

	// Отказ брокера — не таймаут и не закрытие: классы ошибок не должны
	// сливаться, иначе повтор выбирается наугад.
	if errors.Is(err, ErrDeliveryTimeout) || errors.Is(err, ErrProducerClosed) {
		t.Errorf("отказ брокера отнесён к чужому классу ошибок: %v", err)
	}
}

// Спан publish создаёт kotel, а не пакет.
//
// Своего трейсинга у пакета нет, и единственное, что удерживает спан на
// месте, — регистрация хуков kotel в NewKafkaProducer. Тест ловит их пропажу;
// содержимое спана проверяет kotel у себя.
//
//nolint:paralleltest // подменяет глобальный TracerProvider
func TestProducerSendMessageStartsSpan(t *testing.T) {
	brokers := newFakeCluster(t, 1, testTopic)

	tracer := &recordingTracer{}
	prev := otel.GetTracerProvider()

	otel.SetTracerProvider(prodTracerProvider{tracer: tracer})
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	// Провайдер читается в NewKafkaProducer, поэтому продюсер создаётся уже
	// после подмены.
	p := mustProducer(t, testConfig(t, brokers...))

	before := len(tracer.started())

	if err := p.SendMessage(t.Context(), PublishRequest{Topic: testTopic, Value: []byte("v")}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	if got := len(tracer.started()); got <= before {
		t.Fatalf("спанов после отправки = %d, было %d — kotel не подключён к клиенту", got, before)
	}
}
