package encoding

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

var (
	testID1 = uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
	testID2 = uuid.MustParse("b2c3d4e5-f6a7-8901-bcde-f12345678901")
)

const (
	testBotID = "bot-1"
	wantPos0  = "position 0"
)

// Формат каждой части зафиксирован байт в байт: ключи переживают деплой и
// читаются продюсерами на других языках, поэтому кодировка — часть контракта,
// а не деталь реализации. Сравнение по длине такую регрессию не поймает.
func TestEncodeKey_SupportedTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		parts []any
		want  []byte
	}{
		{
			name:  "uuid — 16 байт без длины",
			parts: []any{testID1},
			want:  testID1[:],
		},
		{
			name:  "string — length-prefix big-endian + данные",
			parts: []any{"bot"},
			want:  []byte{0x00, 0x00, 0x00, 0x03, 'b', 'o', 't'},
		},
		{
			name:  "пустая строка — только нулевая длина",
			parts: []any{""},
			want:  []byte{0x00, 0x00, 0x00, 0x00},
		},
		{
			name:  "int64 — 8 байт big-endian",
			parts: []any{int64(42)},
			want:  []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A},
		},
		{
			name:  "int64 ноль",
			parts: []any{int64(0)},
			want:  []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			// Отрицательные значения кодируются как uint64 через
			// reinterpretation, а не как знаковый sign-magnitude.
			name:  "int64 -1 — все биты выставлены",
			parts: []any{int64(-1)},
			want:  []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name:  "int64 MinInt64",
			parts: []any{int64(-1 << 63)},
			want:  []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:  "bool true — один байт 1",
			parts: []any{true},
			want:  []byte{0x01},
		},
		{
			name:  "bool false — один байт 0",
			parts: []any{false},
			want:  []byte{0x00},
		},
		{
			// Пустой список частей — легальный вход: ValidateKeyLength(nil)
			// опирается на нулевую ожидаемую длину.
			name:  "без частей — пустой ключ",
			parts: nil,
			want:  nil,
		},
		{
			name:  "комбинация всех типов подряд",
			parts: []any{testID1, "ab", int64(1), true},
			want: bytes.Join([][]byte{
				testID1[:],
				{0x00, 0x00, 0x00, 0x02, 'a', 'b'},
				{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
				{0x01},
			}, nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := EncodeKey(tt.parts...)
			if err != nil {
				t.Fatalf("EncodeKey() вернул ошибку: %v", err)
			}

			if !bytes.Equal(got, tt.want) {
				t.Errorf("got % X, want % X", got, tt.want)
			}
		})
	}
}

// Длина строки — 4 байта big-endian, а не little-endian и не varint: значение
// больше 255 разложится по байтам только при правильном порядке.
func TestEncodeKey_StringLengthBigEndian(t *testing.T) {
	t.Parallel()

	s := strings.Repeat("x", 258)

	key, err := EncodeKey(s)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if len(key) != 4+len(s) {
		t.Fatalf("len(key) = %d, want %d", len(key), 4+len(s))
	}

	want := []byte{0x00, 0x00, 0x01, 0x02}
	if !bytes.Equal(key[:4], want) {
		t.Errorf("префикс длины: got % X, want % X", key[:4], want)
	}
}

// Один и тот же вход обязан давать один и тот же ключ в пределах процесса и
// между процессами: иначе партиционирование Kafka разъедется и порядок
// сообщений по ключу перестанет соблюдаться.
func TestEncodeKey_Deterministic(t *testing.T) {
	t.Parallel()

	parts := []any{testID1, "hello", int64(99), true}

	first, err := EncodeKey(parts...)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	for i := range 10 {
		next, err := EncodeKey(parts...)
		if err != nil {
			t.Fatalf("EncodeKey() итерация %d вернула ошибку: %v", i, err)
		}

		if !bytes.Equal(first, next) {
			t.Fatalf("итерация %d: got % X, want % X", i, next, first)
		}
	}
}

// Порядок частей значим: ключ — конкатенация, а не множество.
func TestEncodeKey_PartOrderMatters(t *testing.T) {
	t.Parallel()

	direct, err := EncodeKey(testID1, testID2)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	reversed, err := EncodeKey(testID2, testID1)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if bytes.Equal(direct, reversed) {
		t.Fatal("перестановка частей дала тот же ключ, ожидались разные")
	}
}

// Самодостаточность кодировки: наборы частей, дающие одинаковую суммарную
// длину, обязаны давать разные байты — иначе сообщения разных сущностей
// схлопнутся в один ключ. Length-prefix у string существует именно для этого.
func TestEncodeKey_DistinctPartsDistinctBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a    []any
		b    []any
	}{
		{
			// Без префикса длины обе пары склеились бы в "abc".
			name: "разное разбиение одной строки",
			a:    []any{"ab", "c"},
			b:    []any{"a", "bc"},
		},
		{
			name: "строка против её же длины в другом порядке",
			a:    []any{"x", ""},
			b:    []any{"", "x"},
		},
		{
			name: "true/false в одной позиции",
			a:    []any{testID1, true},
			b:    []any{testID1, false},
		},
		{
			name: "соседние int64",
			a:    []any{int64(1)},
			b:    []any{int64(2)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			keyA, err := EncodeKey(tt.a...)
			if err != nil {
				t.Fatalf("EncodeKey(a) вернул ошибку: %v", err)
			}

			keyB, err := EncodeKey(tt.b...)
			if err != nil {
				t.Fatalf("EncodeKey(b) вернул ошибку: %v", err)
			}

			if bytes.Equal(keyA, keyB) {
				t.Errorf("разные наборы дали одинаковый ключ % X", keyA)
			}
		})
	}
}

// Ошибка обязана называть позицию и тип: без них разработчик ищет опечатку
// глазами по всему списку аргументов вызова.
func TestEncodeKey_UnsupportedType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		parts        []any
		wantContains []string
	}{
		{
			name:         "int вместо int64 в первой позиции",
			parts:        []any{42},
			wantContains: []string{"unsupported key part type int", wantPos0},
		},
		{
			name:         "float64 в середине",
			parts:        []any{testID1, 3.14, "tail"},
			wantContains: []string{"unsupported key part type float64", "position 1"},
		},
		{
			name:         "указатель на uuid вместо значения",
			parts:        []any{&testID1},
			wantContains: []string{"*uuid.UUID", wantPos0},
		},
		{
			name:         "[]byte не поддерживается",
			parts:        []any{[]byte{1, 2, 3}},
			wantContains: []string{"[]uint8", wantPos0},
		},
		{
			name:         "nil-часть",
			parts:        []any{nil},
			wantContains: []string{wantPos0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key, err := EncodeKey(tt.parts...)
			if err == nil {
				t.Fatalf("EncodeKey() = % X, ожидалась ошибка", key)
			}

			if key != nil {
				t.Errorf("при ошибке ключ = % X, ожидался nil", key)
			}

			for _, want := range tt.wantContains {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("ошибка %q не содержит %q", err, want)
				}
			}
		})
	}
}

func TestMatchKey_Match(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		parts []any
	}{
		{"один uuid", []any{testID1}},
		{"uuid + string", []any{testID1, testBotID}},
		{"пять частей всех типов", []any{testID1, testID2, "action", int64(7), true}},
		{"только int64", []any{int64(1234567890123)}},
		{"только bool", []any{true}},
		{"пустая строка как часть", []any{testID1, ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key, err := EncodeKey(tt.parts...)
			if err != nil {
				t.Fatalf("EncodeKey() вернул ошибку: %v", err)
			}

			if !MatchKey(key, tt.parts...) {
				t.Errorf("MatchKey() = false, want true")
			}
		})
	}
}

func TestMatchKey_Mismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		encoded []any
		probe   []any
	}{
		{"другая строка", []any{testID1, testBotID}, []any{testID1, "bot-2"}},
		{"другой uuid", []any{testID1, testBotID}, []any{testID2, testBotID}},
		{"другой int64", []any{int64(42)}, []any{int64(43)}},
		{"true против false", []any{true}, []any{false}},
		{"отличается последняя часть из пяти", []any{testID1, testID2, "action", int64(7), true}, []any{testID1, testID2, "action", int64(7), false}},
		{"лишняя часть в конце", []any{testID1}, []any{testID1, "bot"}},
		{"недостающая часть", []any{testID1, "bot"}, []any{testID1}},
		{"те же части в обратном порядке", []any{testID1, testID2}, []any{testID2, testID1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key, err := EncodeKey(tt.encoded...)
			if err != nil {
				t.Fatalf("EncodeKey() вернул ошибку: %v", err)
			}

			if MatchKey(key, tt.probe...) {
				t.Errorf("MatchKey() = true, want false")
			}
		})
	}
}

// Пустой и nil-ключ — это обычное «не совпало», а не особый случай: сообщение
// без ключа не должно ронять обработчик.
func TestMatchKey_EmptyKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   []byte
		parts []any
		want  bool
	}{
		{"nil-ключ против непустых частей", nil, []any{testID1, testBotID}, false},
		{"пустой срез против непустых частей", []byte{}, []any{testID1, testBotID}, false},
		// Без частей ожидаемый ключ пуст, поэтому пустой ключ ему равен.
		{"nil-ключ без частей", nil, nil, true},
		{"пустой срез без частей", []byte{}, nil, true},
		{"непустой ключ без частей", []byte{0x01}, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := MatchKey(tt.key, tt.parts...); got != tt.want {
				t.Errorf("MatchKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Неподдерживаемый тип части — ошибка программиста, а не «ключ не наш».
// Тихий false по godoc-паттерну `if !MatchKey(...) { return nil }` отбрасывал
// бы 100% трафика при зелёных метриках успеха, поэтому здесь именно паника.
func TestMatchKey_UnsupportedTypePanics(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(testID1, "bot")
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("MatchKey() с int не паниковал")
		}

		msg, ok := r.(string)
		if !ok {
			t.Fatalf("паника значением %T, ожидалась строка", r)
		}

		// Текст обязан называть и место, и причину: без них стек в проде
		// разбирается вручную.
		for _, want := range []string{"MatchKey", "unsupported key part type int", "position 1"} {
			if !strings.Contains(msg, want) {
				t.Errorf("паника %q не содержит %q", msg, want)
			}
		}
	}()

	MatchKey(key, testID1, 123)
}

func TestValidateKeyLength_OK(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(testID1, testBotID)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	tests := []struct {
		name  string
		key   []byte
		parts []any
	}{
		{"длина ровно совпадает", key, []any{testID1, testBotID}},
		// Длиннее ожидаемого — не повреждение: это просто чужой ключ, что
		// обнаружит MatchKey, а не проверка длины.
		{"ключ длиннее ожидаемого", key, []any{testID1}},
		{"без частей ожидаемая длина 0", nil, nil},
		{"без частей и непустой ключ", key, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := ValidateKeyLength(tt.key, tt.parts...); err != nil {
				t.Errorf("ValidateKeyLength() = %v, want nil", err)
			}
		})
	}
}

func TestValidateKeyLength_TooShort(t *testing.T) {
	t.Parallel()

	full, err := EncodeKey(testID1, testBotID)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	tests := []struct {
		name string
		key  []byte
	}{
		{"обрезан на один байт", full[:len(full)-1]},
		{"обрезан до половины", full[:len(full)/2]},
		{"пустой срез", []byte{}},
		{"nil", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateKeyLength(tt.key, testID1, testBotID)
			if err == nil {
				t.Fatal("ValidateKeyLength() = nil, ожидалась ошибка")
			}

			// Вызывающий отличает повреждение от чужого тенанта именно по
			// errors.Is, а не по тексту — см. kafkax.MatchKeyMiddleware.
			if !errors.Is(err, ErrInvalidKey) {
				t.Errorf("errors.Is(err, ErrInvalidKey) = false, err: %v", err)
			}
		})
	}
}

// Ошибка кодирования пробрасывается как есть и не маскируется под ErrInvalidKey:
// «неподдерживаемый тип» — баг вызывающего, а не повреждённое сообщение,
// и middleware не должен списывать его на битые данные.
func TestValidateKeyLength_EncodeError(t *testing.T) {
	t.Parallel()

	err := ValidateKeyLength([]byte{1, 2, 3}, 42)
	if err == nil {
		t.Fatal("ValidateKeyLength() = nil, ожидалась ошибка")
	}

	if errors.Is(err, ErrInvalidKey) {
		t.Error("errors.Is(err, ErrInvalidKey) = true, want false")
	}

	if !strings.Contains(err.Error(), "unsupported key part type int") {
		t.Errorf("ошибка %q не содержит причину кодирования", err)
	}
}
