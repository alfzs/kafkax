package kafkax

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/alfzs/kafkax/v3/encoding"
	"github.com/google/uuid"
)

// mwJournal — потокобезопасный журнал меток: на нём проверяется порядок
// вызовов в цепочке. Обычный слайс не годится даже здесь — тесты параллельные,
// а -race ловит запись без синхронизации независимо от фактических гонок.
type mwJournal struct {
	mu    sync.Mutex
	marks []string
}

func (j *mwJournal) mark(s string) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.marks = append(j.marks, s)
}

func (j *mwJournal) snapshot() []string {
	j.mu.Lock()
	defer j.mu.Unlock()

	return slices.Clone(j.marks)
}

// mwTrace возвращает middleware, отмечающее вход и выход вокруг next.
// Пара меток нужна, чтобы отличить вложенность от простой очерёдности:
// «внешний вход, внутренний вход, внутренний выход, внешний выход».
func mwTrace(j *mwJournal, name string) ConsumerMiddleware {
	return func(next ConsumerHandler) ConsumerHandler {
		return ConsumerHandlerFunc(func(ctx context.Context, msg IncomingMessage) error {
			j.mark(name + ":in")

			err := next.ProcessMessage(ctx, msg)

			j.mark(name + ":out")

			return err
		})
	}
}

// mwMsg собирает сообщение с заданным ключом.
func mwMsg(key []byte) IncomingMessage {
	return IncomingMessage{Topic: testTopic, Key: key, Value: []byte("payload")}
}

func TestChainOrder(t *testing.T) {
	t.Parallel()

	j := &mwJournal{}

	handler := ConsumerHandlerFunc(func(context.Context, IncomingMessage) error {
		j.mark("handler")

		return nil
	})

	chained := Chain(handler, mwTrace(j, "first"), mwTrace(j, "second"), mwTrace(j, "third"))

	if err := chained.ProcessMessage(t.Context(), mwMsg(nil)); err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}

	// Первый переданный middleware — внешний. Порядок задан документацией и
	// на нём держатся типовые связки вроде «сначала трейсинг, потом фильтр по
	// ключу»: перевернувшись, фильтр начал бы отсекать сообщения до создания
	// спана, и отброшенные вообще перестали бы быть видны.
	want := []string{
		"first:in", "second:in", "third:in",
		"handler",
		"third:out", "second:out", "first:out",
	}

	if got := j.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("порядок вызовов = %v, want %v", got, want)
	}
}

func TestChainWithoutMiddlewareReturnsHandler(t *testing.T) {
	t.Parallel()

	handler := &mockHandler{}

	// Возврат исходного обработчика без обёрток: AddHandler вызывает Chain
	// всегда, в том числе без middleware, и лишний слой на каждом сообщении
	// был бы платой ни за что.
	if got := Chain(handler); got != ConsumerHandler(handler) {
		t.Fatalf("Chain(handler) вернул %#v, ожидался исходный обработчик", got)
	}

	if got := Chain(handler, []ConsumerMiddleware{}...); got != ConsumerHandler(handler) {
		t.Fatal("Chain с пустым срезом middleware обернул обработчик")
	}
}

func TestChainPropagatesErrorAndContext(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("handler failed")

	type ctxKey struct{}

	var gotValue any

	handler := ConsumerHandlerFunc(func(ctx context.Context, _ IncomingMessage) error {
		gotValue = ctx.Value(ctxKey{})

		return wantErr
	})

	inject := func(next ConsumerHandler) ConsumerHandler {
		return ConsumerHandlerFunc(func(ctx context.Context, msg IncomingMessage) error {
			return next.ProcessMessage(context.WithValue(ctx, ctxKey{}, "from-middleware"), msg)
		})
	}

	err := Chain(handler, inject).ProcessMessage(t.Context(), mwMsg(nil))

	// Ошибка обработчика обязана дойти до консьюмера нетронутой: на ней
	// держатся ретраи и остановка партиции.
	if !errors.Is(err, wantErr) {
		t.Fatalf("ошибка = %v, want %v", err, wantErr)
	}

	if gotValue != "from-middleware" {
		t.Errorf("значение из контекста = %v, want %q", gotValue, "from-middleware")
	}
}

func TestMatchKeyMiddleware(t *testing.T) {
	t.Parallel()

	tenant := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	other := uuid.MustParse("99999999-8888-7777-6666-555555555555")

	matching, err := encoding.EncodeKey(encoding.UUID(tenant), encoding.Int64(7))
	if err != nil {
		t.Fatalf("EncodeKey: %v", err)
	}

	foreign, err := encoding.EncodeKey(encoding.UUID(other), encoding.Int64(7))
	if err != nil {
		t.Fatalf("EncodeKey: %v", err)
	}

	tests := []struct {
		name       string
		key        []byte
		wantCalled bool
		wantErr    error
	}{
		{
			name:       "совпадающий ключ",
			key:        matching,
			wantCalled: true,
		},
		{
			// Чужой тенант — не ошибка: обработчик не вызывается, middleware
			// возвращает nil, оффсет двигается. Иначе топик, который читают
			// все, останавливал бы партицию на первом же чужом сообщении.
			name: "ключ другого тенанта",
			key:  foreign,
		},
		{
			// Ключ длиннее ожидаемого проходит проверку длины, но не
			// совпадает побайтово — то же «не наше», а не повреждение.
			name: "ключ длиннее ожидаемого",
			key:  append(slices.Clone(matching), 0xFF),
		},
		{
			// Короткий ключ — повреждённое или чужого формата сообщение.
			// Молча коммитить его нельзя: по умолчанию оно остановит партицию,
			// а не уедет в тишину.
			name:    "усечённый ключ",
			key:     matching[:len(matching)-1],
			wantErr: encoding.ErrInvalidKey,
		},
		{
			name:    "пустой ключ",
			key:     []byte{},
			wantErr: encoding.ErrInvalidKey,
		},
		{
			name:    "nil-ключ",
			key:     nil,
			wantErr: encoding.ErrInvalidKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			next := &mockHandler{}
			handler := MatchKeyMiddleware(encoding.UUID(tenant), encoding.Int64(7))(next)

			err := handler.ProcessMessage(t.Context(), mwMsg(tt.key))

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("ошибка = %v, want errors.Is(%v)", err, tt.wantErr)
				}

				// Длины в тексте — то, ради чего ошибка оборачивается: без них
				// «invalid composite key» не отличить от опечатки в parts.
				if !strings.Contains(err.Error(), "matching message key") {
					t.Errorf("текст %q не называет операцию", err)
				}
			} else if err != nil {
				t.Fatalf("ProcessMessage = %v, want nil", err)
			}

			called := next.callCount() > 0
			if called != tt.wantCalled {
				t.Fatalf("обработчик вызван = %v, want %v", called, tt.wantCalled)
			}
		})
	}
}

func TestMatchKeyMiddlewarePropagatesHandlerError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("downstream failed")

	key, err := encoding.EncodeKey(encoding.Str("tenant-a"))
	if err != nil {
		t.Fatalf("EncodeKey: %v", err)
	}

	next := &mockHandler{returnErr: wantErr}
	handler := MatchKeyMiddleware(encoding.Str("tenant-a"))(next)

	// Фильтр не должен глотать отказ обработчика: иначе отравленное сообщение
	// своего тенанта тихо коммитилось бы.
	if got := handler.ProcessMessage(t.Context(), mwMsg(key)); !errors.Is(got, wantErr) {
		t.Fatalf("ошибка = %v, want %v", got, wantErr)
	}
}

func TestMatchKeyMiddlewarePanicsAtBuildTime(t *testing.T) {
	t.Parallel()

	// Прежний набор случаев этого теста (int вместо int64, float64, срез байт,
	// структура) больше не выражается в коде: части ключа принимаются как
	// encoding.KeyPart, и MatchKeyMiddleware(uuid.Nil, 42) не компилируется —
	// проверка переехала из рантайма в систему типов.
	//
	// В рантайме остался единственный способ подсунуть невалидную часть —
	// нулевое значение KeyPart, собранное не конструктором (поле структуры,
	// make([]KeyPart, n)). Оно обязано ронять процесс при сборке цепочки, то
	// есть на старте, а не на первом сообщении: иначе фильтр молча разошёлся бы
	// с продюсерским ключом и отбрасывал бы весь трафик.
	tests := []struct {
		name string
		part encoding.KeyPart
	}{
		{name: "нулевое значение KeyPart", part: encoding.KeyPart{}},
		{name: "нулевое значение из среза", part: make([]encoding.KeyPart, 1)[0]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			defer func() {
				recovered := recover()
				if recovered == nil {
					t.Fatal("MatchKeyMiddleware не запаниковал на невалидной части")
				}

				msg, ok := recovered.(string)
				if !ok || !strings.Contains(msg, "MatchKeyMiddleware") {
					t.Fatalf("паника = %v, ожидалось упоминание MatchKeyMiddleware", recovered)
				}

				// Позиция части — то, ради чего паника форматируется: без неё
				// в цепочке из пяти частей ищут глазами.
				if !strings.Contains(msg, "position 1") {
					t.Errorf("паника %q не называет позицию части", msg)
				}
			}()

			// Ни ProcessMessage, ни даже применение к обработчику здесь не
			// вызываются — паника обязана произойти на этой строке.
			_ = MatchKeyMiddleware(encoding.UUID(uuid.Nil), tt.part)

			t.Fatal("недостижимо: сборка middleware завершилась без паники")
		})
	}
}

func TestMatchKeyMiddlewarePanicsWithoutParts(t *testing.T) {
	t.Parallel()

	// Отдельный случай от невалидной части: EncodeKey() без частей
	// ошибки не возвращает — он возвращает пустой ключ. Без явной проверки
	// цепочка собралась бы, и middleware молча отбросил бы весь трафик топика,
	// не оставив в метриках ни одной ошибки.
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("MatchKeyMiddleware не запаниковал на пустом списке частей")
		}

		msg, ok := recovered.(string)
		if !ok || !strings.Contains(msg, "MatchKeyMiddleware") {
			t.Fatalf("паника = %v, ожидалось упоминание MatchKeyMiddleware", recovered)
		}
	}()

	_ = MatchKeyMiddleware()

	t.Fatal("недостижимо: сборка middleware завершилась без паники")
}

func TestMatchKeyMiddlewareInsideChain(t *testing.T) {
	t.Parallel()

	tenant := uuid.MustParse("11111111-2222-3333-4444-555555555555")

	matching, err := encoding.EncodeKey(encoding.UUID(tenant))
	if err != nil {
		t.Fatalf("EncodeKey: %v", err)
	}

	foreign, err := encoding.EncodeKey(encoding.UUID(uuid.MustParse("99999999-8888-7777-6666-555555555555")))
	if err != nil {
		t.Fatalf("EncodeKey: %v", err)
	}

	tests := []struct {
		name       string
		key        []byte
		wantCalled bool
		wantMarks  []string
	}{
		{
			name:       "свой ключ доходит до обработчика",
			key:        matching,
			wantCalled: true,
			wantMarks:  []string{"outer:in", "handler", "outer:out"},
		},
		{
			// Внешнее middleware отрабатывает целиком даже на отброшенном
			// сообщении: пропуск чужого ключа не должен быть невидим для
			// трейсинга и метрик, навешанных снаружи.
			name:      "чужой ключ отсекается внутри цепочки",
			key:       foreign,
			wantMarks: []string{"outer:in", "outer:out"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			j := &mwJournal{}

			handler := ConsumerHandlerFunc(func(context.Context, IncomingMessage) error {
				j.mark("handler")

				return nil
			})

			chained := Chain(handler, mwTrace(j, "outer"), MatchKeyMiddleware(encoding.UUID(tenant)))

			if err := chained.ProcessMessage(t.Context(), mwMsg(tt.key)); err != nil {
				t.Fatalf("ProcessMessage: %v", err)
			}

			if got := j.snapshot(); !slices.Equal(got, tt.wantMarks) {
				t.Fatalf("журнал = %v, want %v", got, tt.wantMarks)
			}
		})
	}
}

// Горячий путь middleware обязан оставаться безаллокационным: части кодируются
// один раз при сборке цепочки, а на сообщение остаются только длина и
// сравнение байтов. Регрессия сюда — вызов encoding.ValidateKeyLength или
// encoding.EncodeKey из ProcessMessage, то есть перекодирование частей на
// каждое сообщение топика.
//
// Тест непараллельный намеренно: testing.AllocsPerRun паникует, если в этот
// момент выполняется хоть один параллельный тест (testing/allocs.go).
//
//nolint:paralleltest // AllocsPerRun несовместим с t.Parallel, см. комментарий выше
func TestMatchKeyMiddlewareDoesNotAllocatePerMessage(t *testing.T) {
	tenant := uuid.MustParse("11111111-2222-3333-4444-555555555555")

	key, err := encoding.EncodeKey(encoding.UUID(tenant), encoding.Int64(7))
	if err != nil {
		t.Fatalf("EncodeKey: %v", err)
	}

	handler := MatchKeyMiddleware(encoding.UUID(tenant), encoding.Int64(7))(&mockHandler{})
	msg := mwMsg(key)
	ctx := t.Context()

	allocs := testing.AllocsPerRun(100, func() {
		if err := handler.ProcessMessage(ctx, msg); err != nil {
			t.Errorf("ProcessMessage: %v", err)
		}
	})

	if allocs > 0 {
		t.Errorf("ProcessMessage = %.0f аллокаций на сообщение, want 0", allocs)
	}
}
