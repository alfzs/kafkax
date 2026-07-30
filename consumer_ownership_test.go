package kafkax

import (
	"context"
	"sync"
	"testing"
)

// Тесты контракта владения памятью IncomingMessage (RF-API-05).
//
// Godoc обещает две вещи, и обе проверяются здесь, а не «на глаз по коду»:
// срез Value переживает возврат из ProcessMessage, и это ТОТ ЖЕ срез, что
// пакет отдаёт в повтор и в OnMessageSkipped. Первое разрешает читать буфер
// после вызова, второе запрещает его мутировать — обещание, снятое с кода,
// а не наоборот.

// sliceRecorder копит указатели на первый байт Value.
//
// Сравниваются именно адреса элементов, а не содержимое: два разных буфера с
// одинаковыми байтами неотличимы по значению, а вопрос стоит ровно про
// разделяемую память.
type sliceRecorder struct {
	mu     sync.Mutex
	firsts []*byte
	values []string
}

func (r *sliceRecorder) add(value []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(value) == 0 {
		r.firsts = append(r.firsts, nil)
	} else {
		r.firsts = append(r.firsts, &value[0])
	}

	r.values = append(r.values, string(value))
}

func (r *sliceRecorder) snapshot() ([]*byte, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]*byte(nil), r.firsts...), append([]string(nil), r.values...)
}

// TestMessageBuffersOutliveHandlerAndAreShared закрывает RF-API-05: прежний
// godoc утверждал, что Key/Value живут «ровно столько, сколько длится вызов
// обработчика», а пакет отдаёт тот же самый срез в повтор и в
// OnMessageSkipped — то есть уже после возврата из ProcessMessage.
//
// Тест фиксирует обе половины нового контракта:
//   - буфер читаем и не испорчен после возврата из обработчика (хук видит
//     исходное значение);
//   - буфер разделяемый — адрес первого байта совпадает во всех трёх точках,
//     поэтому мутация на месте была бы видна следующей попытке и DLQ.
func TestMessageBuffersOutliveHandlerAndAreShared(t *testing.T) {
	t.Parallel()

	const topic = "kafkax-ownership-topic"

	brokers := newFakeCluster(t, 1, topic)
	cfg := testConfig(t, brokers...)

	// Один повтор: сравнивать адреса между попытками имеет смысл только если
	// попыток больше одной.
	cfg.Consumer.HandlerMaxRetries = 1

	hook := &sliceRecorder{}
	cfg.OnMessageSkipped = func(_ context.Context, msg IncomingMessage, _ error) error {
		hook.add(msg.Value)

		return nil
	}

	prod := consNewProducer(t, brokers)
	prod.send(t, topic, 0, consPoisonValue)

	handler := &sliceRecorder{}
	h := &mockHandler{fn: func(_ int, msg IncomingMessage) error {
		handler.add(msg.Value)

		return errConsBoom
	}}

	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, topic, h)
	consStart(t, c)

	waitFor(t, consWait, "хук получил сообщение после исчерпания повторов", func() bool {
		ptrs, _ := hook.snapshot()

		return len(ptrs) == 1
	})

	handlerPtrs, handlerValues := handler.snapshot()
	hookPtrs, hookValues := hook.snapshot()

	if len(handlerPtrs) != 2 {
		t.Fatalf("вызовов обработчика = %d, want 2 (первый плюс один повтор)", len(handlerPtrs))
	}

	// Содержимое цело в хуке — значит, буфер не переиспользован и не обнулён
	// после возврата из ProcessMessage. Именно это разрешает читать Value
	// после вызова, не копируя.
	if hookValues[0] != consPoisonValue {
		t.Fatalf("OnMessageSkipped получил %q, want %q: буфер испортился после возврата из обработчика",
			hookValues[0], consPoisonValue)
	}

	if handlerValues[0] != consPoisonValue || handlerValues[1] != consPoisonValue {
		t.Fatalf("обработчик получил %v, want два раза %q", handlerValues, consPoisonValue)
	}

	// Ядро контракта: один и тот же массив во всех трёх точках. Если это
	// когда-нибудь перестанет быть правдой, запрет на мутацию Value можно будет
	// снять — но godoc придётся править вместе с этим тестом.
	if handlerPtrs[0] != handlerPtrs[1] {
		t.Fatal("повтор получил другой буфер: обещание «тот же срез уходит в повторы» неверно")
	}

	if hookPtrs[0] != handlerPtrs[0] {
		t.Fatal("OnMessageSkipped получил другой буфер: обещание «тот же срез уходит в хук» неверно")
	}
}
