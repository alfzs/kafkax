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
		parts []KeyPart
		want  []byte
	}{
		{
			name:  "uuid — 16 байт без длины",
			parts: []KeyPart{UUID(testID1)},
			want:  testID1[:],
		},
		{
			name:  "string — length-prefix big-endian + данные",
			parts: []KeyPart{Str("bot")},
			want:  []byte{0x00, 0x00, 0x00, 0x03, 'b', 'o', 't'},
		},
		{
			name:  "пустая строка — только нулевая длина",
			parts: []KeyPart{Str("")},
			want:  []byte{0x00, 0x00, 0x00, 0x00},
		},
		{
			name:  "int64 — 8 байт big-endian",
			parts: []KeyPart{Int64(42)},
			want:  []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A},
		},
		{
			name:  "int64 ноль",
			parts: []KeyPart{Int64(0)},
			want:  []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			// Отрицательные значения кодируются как uint64 через
			// reinterpretation, а не как знаковый sign-magnitude.
			name:  "int64 -1 — все биты выставлены",
			parts: []KeyPart{Int64(-1)},
			want:  []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name:  "int64 MinInt64",
			parts: []KeyPart{Int64(-1 << 63)},
			want:  []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:  "bool true — один байт 1",
			parts: []KeyPart{Bool(true)},
			want:  []byte{0x01},
		},
		{
			name:  "bool false — один байт 0",
			parts: []KeyPart{Bool(false)},
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
			parts: []KeyPart{UUID(testID1), Str("ab"), Int64(1), Bool(true)},
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

	key, err := EncodeKey(Str(s))
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

	parts := []KeyPart{UUID(testID1), Str("hello"), Int64(99), Bool(true)}

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

	direct, err := EncodeKey(UUID(testID1), UUID(testID2))
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	reversed, err := EncodeKey(UUID(testID2), UUID(testID1))
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
		a    []KeyPart
		b    []KeyPart
	}{
		{
			// Без префикса длины обе пары склеились бы в "abc".
			name: "разное разбиение одной строки",
			a:    []KeyPart{Str("ab"), Str("c")},
			b:    []KeyPart{Str("a"), Str("bc")},
		},
		{
			name: "строка против её же длины в другом порядке",
			a:    []KeyPart{Str("x"), Str("")},
			b:    []KeyPart{Str(""), Str("x")},
		},
		{
			name: "true/false в одной позиции",
			a:    []KeyPart{UUID(testID1), Bool(true)},
			b:    []KeyPart{UUID(testID1), Bool(false)},
		},
		{
			name: "соседние int64",
			a:    []KeyPart{Int64(1)},
			b:    []KeyPart{Int64(2)},
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

// Неподдерживаемый тип части больше не существует как класс ошибки: части
// собираются конструкторами, и EncodeKey(42) не компилируется. Остался ровно
// один способ подсунуть мусор — нулевое значение KeyPart (например, из
// make([]KeyPart, n)). Он обязан быть отвергнут с позицией и сентинелом, а не
// закодирован как пустая часть: такой ключ молча разъехался бы с продюсерским.
func TestEncodeKey_InvalidPart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		parts        []KeyPart
		wantContains []string
	}{
		{
			name:         "нулевое значение KeyPart в первой позиции",
			parts:        []KeyPart{{}},
			wantContains: []string{"zero-value KeyPart", wantPos0},
		},
		{
			name:         "нулевое значение в середине",
			parts:        []KeyPart{UUID(testID1), {}, Str("tail")},
			wantContains: []string{"zero-value KeyPart", "position 1"},
		},
		{
			// Неизвестный kind недостижим снаружи пакета, но ветка обязана
			// отказывать, а не писать байты: добавление нового вида части без
			// правки appendTo не должно давать молча усечённый ключ.
			name:         "неизвестный kind",
			parts:        []KeyPart{{kind: 200}},
			wantContains: []string{"unknown kind 200", wantPos0},
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

			// Вызывающий отличает баг в своём коде от повреждённого сообщения
			// по сентинелу, а не по тексту.
			if !errors.Is(err, ErrInvalidKeyPart) {
				t.Errorf("errors.Is(err, ErrInvalidKeyPart) = false, err: %v", err)
			}

			for _, want := range tt.wantContains {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("ошибка %q не содержит %q", err, want)
				}
			}
		})
	}
}

// Вторая половина двухпроходной сборки: appendTo вызывается только после
// успешного size, поэтому на невалидной части он обязан ничего не писать.
// Иначе ошибка в порядке вызовов дала бы ключ с мусорными байтами.
func TestKeyPart_AppendToInvalidWritesNothing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		part KeyPart
	}{
		{name: "нулевое значение", part: KeyPart{}},
		{name: "неизвестный kind", part: KeyPart{kind: 200}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			buf := []byte{0xAA}

			if got := tt.part.appendTo(buf); !bytes.Equal(got, buf) {
				t.Errorf("appendTo дописал % X, ожидался неизменный буфер", got)
			}
		})
	}
}

// RF-PERF-05: размер считается первым проходом, запись идёт в один буфер.
// Регрессия сюда возвращает по два make на часть и рост буфера из nil, то есть
// четыре аллокации на ключ у всех, кто кодирует его на каждое сообщение.
//
// Ни сам тест, ни его случаи не параллельны намеренно: testing.AllocsPerRun
// паникует, если в этот момент выполняется хоть один параллельный тест
// (testing/allocs.go). Подслучаи развёрнуты в цикл без t.Run по той же причине.
//
//nolint:paralleltest // AllocsPerRun несовместим с t.Parallel, см. комментарий выше
func TestEncodeKey_SingleAllocation(t *testing.T) {
	tests := []struct {
		name  string
		parts []KeyPart
	}{
		{name: "два uuid", parts: []KeyPart{UUID(testID1), UUID(testID2)}},
		{
			name:  "uuid + строка + int64 + bool",
			parts: []KeyPart{UUID(testID1), Str(testBotID), Int64(7), Bool(true)},
		},
	}

	for _, tt := range tests {
		var key []byte

		allocs := testing.AllocsPerRun(100, func() {
			encoded, err := EncodeKey(tt.parts...)
			if err != nil {
				t.Errorf("%s: EncodeKey() вернул ошибку: %v", tt.name, err)
			}

			key = encoded
		})

		if key == nil {
			t.Fatalf("%s: ключ не собран", tt.name)
		}

		if allocs > 1 {
			t.Errorf("%s: EncodeKey() = %.0f аллокаций, want 1", tt.name, allocs)
		}
	}
}

// Буфер выделяется ровно под итоговый размер: лишняя ёмкость означала бы, что
// первый проход считает не то, что пишет второй.
func TestEncodeKey_ExactCapacity(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(UUID(testID1), Str(testBotID), Int64(7), Bool(true))
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	want := uuidSize + lenPrefixSize + len(testBotID) + int64Size + boolSize
	if len(key) != want {
		t.Fatalf("len(key) = %d, want %d", len(key), want)
	}

	if cap(key) != want {
		t.Errorf("cap(key) = %d, want %d — буфер не преаллоцирован точно", cap(key), want)
	}
}

func TestMatchKey_Match(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		parts []KeyPart
	}{
		{"один uuid", []KeyPart{UUID(testID1)}},
		{"uuid + string", []KeyPart{UUID(testID1), Str(testBotID)}},
		{
			"пять частей всех типов",
			[]KeyPart{UUID(testID1), UUID(testID2), Str("action"), Int64(7), Bool(true)},
		},
		{"только int64", []KeyPart{Int64(1234567890123)}},
		{"только bool", []KeyPart{Bool(true)}},
		{"пустая строка как часть", []KeyPart{UUID(testID1), Str("")}},
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
		encoded []KeyPart
		probe   []KeyPart
	}{
		{
			"другая строка",
			[]KeyPart{UUID(testID1), Str(testBotID)},
			[]KeyPart{UUID(testID1), Str("bot-2")},
		},
		{
			"другой uuid",
			[]KeyPart{UUID(testID1), Str(testBotID)},
			[]KeyPart{UUID(testID2), Str(testBotID)},
		},
		{"другой int64", []KeyPart{Int64(42)}, []KeyPart{Int64(43)}},
		{"true против false", []KeyPart{Bool(true)}, []KeyPart{Bool(false)}},
		{
			"отличается последняя часть из пяти",
			[]KeyPart{UUID(testID1), UUID(testID2), Str("action"), Int64(7), Bool(true)},
			[]KeyPart{UUID(testID1), UUID(testID2), Str("action"), Int64(7), Bool(false)},
		},
		{"лишняя часть в конце", []KeyPart{UUID(testID1)}, []KeyPart{UUID(testID1), Str("bot")}},
		{"недостающая часть", []KeyPart{UUID(testID1), Str("bot")}, []KeyPart{UUID(testID1)}},
		{
			"те же части в обратном порядке",
			[]KeyPart{UUID(testID1), UUID(testID2)},
			[]KeyPart{UUID(testID2), UUID(testID1)},
		},
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
		parts []KeyPart
		want  bool
	}{
		{"nil-ключ против непустых частей", nil, []KeyPart{UUID(testID1), Str(testBotID)}, false},
		{
			"пустой срез против непустых частей",
			[]byte{},
			[]KeyPart{UUID(testID1), Str(testBotID)},
			false,
		},
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

// Невалидная часть — ошибка программиста, а не «ключ не наш». Тихий false по
// godoc-паттерну `if !MatchKey(...) { return nil }` отбрасывал бы 100% трафика
// при зелёных метриках успеха, поэтому здесь именно паника.
func TestMatchKey_InvalidPartPanics(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(UUID(testID1), Str("bot"))
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("MatchKey() с нулевой частью не паниковал")
		}

		msg, ok := r.(string)
		if !ok {
			t.Fatalf("паника значением %T, ожидалась строка", r)
		}

		// Текст обязан называть и место, и причину: без них стек в проде
		// разбирается вручную.
		for _, want := range []string{"MatchKey", "zero-value KeyPart", "position 1"} {
			if !strings.Contains(msg, want) {
				t.Errorf("паника %q не содержит %q", msg, want)
			}
		}
	}()

	MatchKey(key, UUID(testID1), KeyPart{})
}

// Предкодированный Key — то, ради чего существует тип: части кодируются один
// раз, дальше только сравнения. Байты обязаны совпадать с EncodeKey, иначе
// консьюмер на Key и продюсер на EncodeKey разъедутся.
func TestNewKey_BytesMatchEncodeKey(t *testing.T) {
	t.Parallel()

	parts := []KeyPart{UUID(testID1), Str(testBotID), Int64(7), Bool(false)}

	encoded, err := EncodeKey(parts...)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	key, err := NewKey(parts...)
	if err != nil {
		t.Fatalf("NewKey() вернул ошибку: %v", err)
	}

	if !bytes.Equal(key.Bytes(), encoded) {
		t.Errorf("Key.Bytes() = % X, want % X", key.Bytes(), encoded)
	}

	if !key.Match(encoded) {
		t.Error("Key.Match() = false для собственных байтов")
	}

	if err := key.ValidateLength(encoded); err != nil {
		t.Errorf("Key.ValidateLength() = %v, want nil", err)
	}
}

// Ошибка кодирования не должна давать полуготовый Key: с пустым raw он
// совпадал бы с любым пустым ключом и пропускал бы весь трафик.
func TestNewKey_InvalidPart(t *testing.T) {
	t.Parallel()

	key, err := NewKey(UUID(testID1), KeyPart{})
	if err == nil {
		t.Fatal("NewKey() = nil, ожидалась ошибка")
	}

	if !errors.Is(err, ErrInvalidKeyPart) {
		t.Errorf("errors.Is(err, ErrInvalidKeyPart) = false, err: %v", err)
	}

	if key.Bytes() != nil {
		t.Errorf("при ошибке Key.Bytes() = % X, ожидался nil", key.Bytes())
	}
}

// Нулевой Key — это пустой ключ, а не «любой»: без частей ожидаемая длина 0,
// поэтому проверка длины пропускает всё, а сравнение — только пустой ключ.
func TestKey_Zero(t *testing.T) {
	t.Parallel()

	var key Key

	if err := key.ValidateLength([]byte{1, 2, 3}); err != nil {
		t.Errorf("ValidateLength() = %v, want nil", err)
	}

	if !key.Match(nil) {
		t.Error("Match(nil) = false, want true")
	}

	if key.Match([]byte{1}) {
		t.Error("Match(непустой) = true, want false")
	}
}

func TestValidateKeyLength_OK(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(UUID(testID1), Str(testBotID))
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	tests := []struct {
		name  string
		key   []byte
		parts []KeyPart
	}{
		{"длина ровно совпадает", key, []KeyPart{UUID(testID1), Str(testBotID)}},
		// Длиннее ожидаемого — не повреждение: это просто чужой ключ, что
		// обнаружит MatchKey, а не проверка длины.
		{"ключ длиннее ожидаемого", key, []KeyPart{UUID(testID1)}},
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

	full, err := EncodeKey(UUID(testID1), Str(testBotID))
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

			err := ValidateKeyLength(tt.key, UUID(testID1), Str(testBotID))
			if err == nil {
				t.Fatal("ValidateKeyLength() = nil, ожидалась ошибка")
			}

			// Вызывающий отличает повреждение от чужого тенанта именно по
			// errors.Is, а не по тексту — см. kafkax.MatchKeyMiddleware.
			if !errors.Is(err, ErrInvalidKey) {
				t.Errorf("errors.Is(err, ErrInvalidKey) = false, err: %v", err)
			}

			// Длины в тексте — то, ради чего ошибка форматируется: без них
			// «invalid composite key» не отличить от опечатки в частях.
			if !strings.Contains(err.Error(), "want at least") {
				t.Errorf("ошибка %q не называет ожидаемую длину", err)
			}
		})
	}
}

// Ошибка кодирования пробрасывается как есть и не маскируется под ErrInvalidKey:
// невалидная часть — баг вызывающего, а не повреждённое сообщение,
// и middleware не должен списывать его на битые данные.
func TestValidateKeyLength_EncodeError(t *testing.T) {
	t.Parallel()

	err := ValidateKeyLength([]byte{1, 2, 3}, KeyPart{})
	if err == nil {
		t.Fatal("ValidateKeyLength() = nil, ожидалась ошибка")
	}

	if errors.Is(err, ErrInvalidKey) {
		t.Error("errors.Is(err, ErrInvalidKey) = true, want false")
	}

	if !errors.Is(err, ErrInvalidKeyPart) {
		t.Errorf("errors.Is(err, ErrInvalidKeyPart) = false, err: %v", err)
	}
}
