package kafkax

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Тесты сентинелов и перевода ошибок franz-go в них.
//
// Контракт пакета — переменные, а не тексты: вызывающий обязан различать
// «сообщение точно не ушло» и «сообщение могло уйти» через errors.Is, не
// разбирая строки. Всё, что здесь проверяется, — устойчивость этого различия.

// errSentinels — все экспортируемые сентинелы пакета.
var errSentinels = map[string]error{
	"ErrProducerClosed":    ErrProducerClosed,
	"ErrDeliveryTimeout":   ErrDeliveryTimeout,
	"ErrDeliveryFailed":    ErrDeliveryFailed,
	"ErrHandlerPanic":      ErrHandlerPanic,
	"ErrConsumerClosed":    ErrConsumerClosed,
	"ErrConsumerStarted":   ErrConsumerStarted,
	"ErrNoHandlers":        ErrNoHandlers,
	"ErrEmptyTopic":        ErrEmptyTopic,
	"ErrEmptyHeaderKey":    ErrEmptyHeaderKey,
	"ErrReservedHeaderKey": ErrReservedHeaderKey,
	"ErrNilHandler":        ErrNilHandler,
}

// errProduceError вызывает маппинг ошибок продюсера на нулевом значении.
//
// produceError — чистая функция от аргумента: она не трогает ни клиента, ни
// состояние продюсера, поэтому тесту маппинга не нужен ни брокер, ни
// конструктор. Если функция когда-нибудь начнёт читать поля получателя, этот
// вызов упадёт паникой — что и будет сигналом пересмотреть тест.
func errProduceError(err error) error {
	return (&KafkaProducer{}).produceError(testTopic, err)
}

func TestSentinelsAreDistinct(t *testing.T) {
	t.Parallel()

	for nameA, a := range errSentinels {
		for nameB, b := range errSentinels {
			if nameA == nameB {
				continue
			}

			// Совпадение двух сентинелов означало бы, что вызывающий не может
			// отличить, например, закрытый продюсер от таймаута доставки — а
			// это ровно то решение, ради которого сентинелы и существуют.
			if errors.Is(a, b) {
				t.Errorf("errors.Is(%s, %s) == true — сентинелы неразличимы", nameA, nameB)
			}
		}

		if a.Error() == "" {
			t.Errorf("%s имеет пустой текст", nameA)
		}

		// Префикс пакета в тексте: ошибка всплывает в чужих логах, и без него
		// «producer is shutting down» нечем атрибутировать.
		if !strings.HasPrefix(a.Error(), "kafkax: ") {
			t.Errorf("%s = %q, ожидался префикс \"kafkax: \"", nameA, a.Error())
		}
	}
}

func TestSentinelsSurviveWrapping(t *testing.T) {
	t.Parallel()

	for name, sentinel := range errSentinels {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Двойная обёртка: библиотека добавляет контекст на каждом уровне,
			// и errors.Is обязан находить сентинел на любой глубине.
			wrapped := fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", sentinel))

			if !errors.Is(wrapped, sentinel) {
				t.Fatalf("errors.Is не нашёл %s под двумя обёртками", name)
			}

			if errors.Is(wrapped, ErrNoHandlers) && !errors.Is(sentinel, ErrNoHandlers) {
				t.Fatalf("обёртка над %s ошибочно опознана как ErrNoHandlers", name)
			}
		})
	}
}

// TestProduceErrorMapping — сентинел И причина в каждой ветке produceError.
//
// Сентинел отвечает на вопрос «повторять ли», причина — «что именно
// случилось». Раньше две ветки из четырёх возвращали голый сентинел, и
// «наш дедлайн истёк» было не отличить от «клиент не смог дослать запись»:
// диагнозы разные, лечение разное, а ошибка одна и та же.
func TestProduceErrorMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		in        error
		want      error
		wantCause error
	}{
		{
			// Дедлайн контекста и таймаут записи означают одно и то же для
			// вызывающего: запись уже у клиента и МОГЛА доехать, поэтому
			// повтор способен создать дубликат. Различает их причина.
			name:      "дедлайн контекста",
			in:        context.DeadlineExceeded,
			want:      ErrDeliveryTimeout,
			wantCause: context.DeadlineExceeded,
		},
		{
			name:      "kgo.ErrRecordTimeout",
			in:        kgo.ErrRecordTimeout,
			want:      ErrDeliveryTimeout,
			wantCause: kgo.ErrRecordTimeout,
		},
		{
			name:      "обёрнутый kgo.ErrRecordTimeout",
			in:        fmt.Errorf("produce: %w", kgo.ErrRecordTimeout),
			want:      ErrDeliveryTimeout,
			wantCause: kgo.ErrRecordTimeout,
		},
		{
			// Close успел закрыть клиент между acquire и ProduceSync: с точки
			// зрения вызывающего это тот же «продюсер закрыт», что и
			// проваленная проверка в acquire, и повтор здесь бессмыслен.
			name:      "kgo.ErrClientClosed",
			in:        kgo.ErrClientClosed,
			want:      ErrProducerClosed,
			wantCause: kgo.ErrClientClosed,
		},
		{
			name:      "kgo.ErrAborting",
			in:        kgo.ErrAborting,
			want:      ErrProducerClosed,
			wantCause: kgo.ErrAborting,
		},
		{
			name:      "отказ брокера",
			in:        kerr.NotEnoughReplicas,
			want:      ErrDeliveryFailed,
			wantCause: kerr.NotEnoughReplicas,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := errProduceError(tt.in)

			if !errors.Is(got, tt.want) {
				t.Fatalf("produceError(%v) = %v, want errors.Is(%v)", tt.in, got, tt.want)
			}

			if !errors.Is(got, tt.wantCause) {
				t.Fatalf("produceError(%v) = %v: причина %v потеряна", tt.in, got, tt.wantCause)
			}
		})
	}
}

// TestProduceErrorSeparatesTimeoutCauses — один сентинел, две разные причины.
//
// Ровно тот случай, ради которого RF-API-07 и заведён: ErrDeliveryTimeout
// приходит и на исчерпанный бюджет вызова, и на неспособность клиента дослать
// запись. Первое лечится увеличением MessageTimeout, второе — разбирательством
// с кластером, и путать их нельзя.
func TestProduceErrorSeparatesTimeoutCauses(t *testing.T) {
	t.Parallel()

	byDeadline := errProduceError(context.DeadlineExceeded)
	byRecord := errProduceError(kgo.ErrRecordTimeout)

	if !errors.Is(byDeadline, ErrDeliveryTimeout) || !errors.Is(byRecord, ErrDeliveryTimeout) {
		t.Fatalf("оба исхода обязаны нести ErrDeliveryTimeout: %v / %v", byDeadline, byRecord)
	}

	if errors.Is(byDeadline, kgo.ErrRecordTimeout) {
		t.Errorf("дедлайн контекста опознан как kgo.ErrRecordTimeout: %v", byDeadline)
	}

	if errors.Is(byRecord, context.DeadlineExceeded) {
		t.Errorf("kgo.ErrRecordTimeout опознан как дедлайн контекста: %v", byRecord)
	}
}

func TestProduceErrorPropagatesCancellation(t *testing.T) {
	t.Parallel()

	got := errProduceError(context.Canceled)

	// Отмена — решение вызывающего, а не отказ Kafka, и подменять её
	// сентинелом доставки нельзя: вызывающий должен узнать свой context.
	if !errors.Is(got, context.Canceled) {
		t.Fatalf("produceError(context.Canceled) = %v, ожидался context.Canceled", got)
	}

	for name, sentinel := range errSentinels {
		if errors.Is(got, sentinel) {
			t.Errorf("отмена контекста опознана как %s", name)
		}
	}

	// Префикс называет операцию, а не причину: ctx.Done() срабатывает и на
	// отмене, и на дедлайне, и «context canceled: context deadline exceeded»
	// противоречило бы само себе.
	if !strings.Contains(got.Error(), "send message") {
		t.Errorf("текст %q не называет операцию", got)
	}
}

func TestProduceErrorKeepsBrokerError(t *testing.T) {
	t.Parallel()

	// Двойной %w в produceError — не украшение: errors.Is находит сентинел, по
	// которому решается «повторять ли вообще», а errors.As достаёт
	// *DeliveryError с кодом брокера, по которому видно, имеет ли повтор смысл
	// (MessageTooLarge — нет, NotEnoughReplicas — да). Потеря любой из двух
	// возможностей ломает половину этого решения, поэтому проверяются обе.
	tests := []struct {
		name          string
		in            *kerr.Error
		wantRetriable bool
	}{
		{name: "MessageTooLarge", in: kerr.MessageTooLarge, wantRetriable: false},
		{name: "NotEnoughReplicas", in: kerr.NotEnoughReplicas, wantRetriable: true},
		{name: "UnknownTopicOrPartition", in: kerr.UnknownTopicOrPartition, wantRetriable: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := errProduceError(tt.in)

			if !errors.Is(got, ErrDeliveryFailed) {
				t.Errorf("errors.Is(err, ErrDeliveryFailed) == false для %v", tt.in)
			}

			var delivery *DeliveryError
			if !errors.As(got, &delivery) {
				t.Fatalf("errors.As не достал *DeliveryError из %v", got)
			}

			if delivery.Code != tt.in.Code {
				t.Errorf("код брокера = %d, want %d", delivery.Code, tt.in.Code)
			}

			if delivery.Name != tt.in.Message {
				t.Errorf("имя кода = %q, want %q", delivery.Name, tt.in.Message)
			}

			// Признак повторяемости — то единственное, ради чего потребитель и
			// разбирает ошибку; брать его из чужого типа он больше не обязан.
			if delivery.Retriable != tt.wantRetriable {
				t.Errorf("Retriable = %t, want %t", delivery.Retriable, tt.wantRetriable)
			}

			if delivery.Topic != testTopic {
				t.Errorf("Topic = %q, want %q", delivery.Topic, testTopic)
			}

			// Ошибка клиента остаётся достижимой через Unwrap: она деталь
			// реализации, но обрывать цепочку было бы враньём.
			var kerrErr *kerr.Error
			if !errors.As(got, &kerrErr) || kerrErr.Code != tt.in.Code {
				t.Errorf("исходная ошибка клиента недостижима через цепочку: %v", got)
			}

			// Прочие сентинелы доставки не должны срабатывать: иначе отказ
			// брокера принимали бы за таймаут и молча повторяли.
			if errors.Is(got, ErrDeliveryTimeout) || errors.Is(got, ErrProducerClosed) {
				t.Errorf("отказ брокера опознан как таймаут или закрытие: %v", got)
			}
		})
	}
}

func TestProduceErrorUnwrapsWrappedBrokerError(t *testing.T) {
	t.Parallel()

	// franz-go отдаёт ошибку брокера завёрнутой в собственный контекст —
	// диагностика не должна ломаться от лишнего слоя.
	in := fmt.Errorf("produce to topic %q: %w", testTopic, kerr.NotEnoughReplicas)

	got := errProduceError(in)

	var delivery *DeliveryError
	if !errors.As(got, &delivery) || delivery.Code != kerr.NotEnoughReplicas.Code {
		t.Fatalf("не удалось достать код брокера из %v", got)
	}

	if !errors.Is(got, ErrDeliveryFailed) {
		t.Fatalf("errors.Is(err, ErrDeliveryFailed) == false для %v", got)
	}
}

// TestDeliveryErrorWithoutBrokerCode — отказ, до брокера не доехавший.
//
// Сеть, TLS и разрешение имени кода протокола не дают, и DeliveryError обязан
// честно сказать «кода нет» (Code == 0, Retriable == false), а не выдумать
// значение: «клиент не сказал» и «повторять бессмысленно» — разные вещи.
func TestDeliveryErrorWithoutBrokerCode(t *testing.T) {
	t.Parallel()

	cause := errors.New("dial tcp: connection refused")

	got := errProduceError(cause)

	var delivery *DeliveryError
	if !errors.As(got, &delivery) {
		t.Fatalf("errors.As не достал *DeliveryError из %v", got)
	}

	if delivery.Code != 0 || delivery.Name != "" || delivery.Retriable {
		t.Errorf("DeliveryError = %+v, ожидались нулевой код и Retriable=false", delivery)
	}

	if !errors.Is(got, cause) {
		t.Errorf("причина %v потеряна в %v", cause, got)
	}

	if !strings.Contains(got.Error(), testTopic) {
		t.Errorf("текст %q не называет топик", got)
	}
}

// TestDeliveryErrorIsSentinelWithoutWrapping — errors.Is работает и на
// *DeliveryError, полученной без обёртки: связь типа с сентинелом даёт метод
// Is, а не место конструирования.
func TestDeliveryErrorIsSentinelWithoutWrapping(t *testing.T) {
	t.Parallel()

	err := error(&DeliveryError{Topic: testTopic, Err: errors.New("boom")})

	if !errors.Is(err, ErrDeliveryFailed) {
		t.Fatalf("errors.Is(%v, ErrDeliveryFailed) == false", err)
	}

	if errors.Is(err, ErrDeliveryTimeout) {
		t.Fatalf("*DeliveryError опознана как ErrDeliveryTimeout: %v", err)
	}

	if got := err.Error(); !strings.Contains(got, "boom") {
		t.Errorf("текст %q не доносит причину", got)
	}
}
