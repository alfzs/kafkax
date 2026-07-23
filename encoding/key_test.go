package encoding

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestEncodeKey_UUID(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

	key, err := EncodeKey(id)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if len(key) != 16 {
		t.Fatalf("EncodeKey() len = %d, ожидалось 16", len(key))
	}

	gotID, err := uuid.FromBytes(key)
	if err != nil {
		t.Fatalf("uuid.FromBytes() ошибка: %v", err)
	}

	if gotID != id {
		t.Fatalf("id = %v, ожидалось %v", gotID, id)
	}
}

func TestEncodeKey_String(t *testing.T) {
	t.Parallel()

	s := "my-bot-id"

	key, err := EncodeKey(s)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	// формат: [4 bytes length][N bytes data]
	// длина = len(key) - 4
	if len(key) != 4+len(s) {
		t.Fatalf("EncodeKey() len = %d, ожидалось %d", len(key), 4+len(s))
	}

	gotLen := uint32(key[0])<<24 | uint32(key[1])<<16 | uint32(key[2])<<8 | uint32(key[3])
	if gotLen != uint32(len(s)) { //nolint:gosec // test fixture, len(s) is trivially small
		t.Fatalf("string length = %d, ожидалось %d", gotLen, len(s))
	}

	gotS := string(key[4:])
	if gotS != s {
		t.Fatalf("string = %q, ожидалось %q", gotS, s)
	}
}

func TestEncodeKey_StringEmpty(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey("")
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if len(key) != 4 {
		t.Fatalf("EncodeKey() с пустой строкой len = %d, ожидалось 4", len(key))
	}
}

func TestEncodeKey_Int64(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(int64(42))
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if len(key) != 8 {
		t.Fatalf("EncodeKey() len = %d, ожидалось 8", len(key))
	}
}

func TestEncodeKey_Int64Zero(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(int64(0))
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if len(key) != 8 {
		t.Fatalf("EncodeKey() len = %d, ожидалось 8", len(key))
	}

	got := int64(key[0])<<56 | int64(key[1])<<48 | int64(key[2])<<40 | int64(key[3])<<32 |
		int64(key[4])<<24 | int64(key[5])<<16 | int64(key[6])<<8 | int64(key[7])

	if got != 0 {
		t.Fatalf("int64 = %d, ожидалось 0", got)
	}
}

func TestEncodeKey_Int64Negative(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(int64(-1))
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if len(key) != 8 {
		t.Fatalf("EncodeKey() len = %d, ожидалось 8", len(key))
	}

	got := int64(key[0])<<56 | int64(key[1])<<48 | int64(key[2])<<40 | int64(key[3])<<32 |
		int64(key[4])<<24 | int64(key[5])<<16 | int64(key[6])<<8 | int64(key[7])

	if got != -1 {
		t.Fatalf("int64 = %d, ожидалось -1", got)
	}
}

func TestEncodeKey_Bool(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(true)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if len(key) != 1 {
		t.Fatalf("EncodeKey() len = %d, ожидалось 1", len(key))
	}

	if key[0] != 1 {
		t.Fatalf("bool = %d, ожидалось 1", key[0])
	}
}

func TestEncodeKey_BoolFalse(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(false)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if len(key) != 1 {
		t.Fatalf("EncodeKey() len = %d, ожидалось 1", len(key))
	}

	if key[0] != 0 {
		t.Fatalf("bool = %d, ожидалось 0", key[0])
	}
}

func TestEncodeKey_MultiPart(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
	s := "my-bot"
	n := int64(42)

	key, err := EncodeKey(id, s, n)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	// UUID=16 + string=4+6 + int64=8 = 34
	if len(key) != 34 {
		t.Fatalf("EncodeKey() len = %d, ожидалось 34", len(key))
	}
}

func TestEncodeKey_FourParts(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
	id2 := uuid.MustParse("b2c3d4e5-f6a7-8901-bcde-f12345678901")

	key, err := EncodeKey(id1, id2, "action", true)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	// UUID=16 + UUID=16 + string=4+6 + bool=1 = 43
	if len(key) != 43 {
		t.Fatalf("EncodeKey() len = %d, ожидалось 43", len(key))
	}
}

func TestEncodeKey_UnsupportedType(t *testing.T) {
	t.Parallel()

	_, err := EncodeKey(42) // int — не поддерживается
	if err == nil {
		t.Fatal("EncodeKey(int) вернул nil, ожидалась ошибка")
	}
}

func TestEncodeKey_UnsupportedTypeMiddle(t *testing.T) {
	t.Parallel()

	id := uuid.New()

	_, err := EncodeKey(id, 3.14) // float64 — не поддерживается
	if err == nil {
		t.Fatal("EncodeKey() с float64 вернул nil, ожидалась ошибка")
	}
}

func TestMatchKey_Exact(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

	key, err := EncodeKey(id, "bot-1")
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if !MatchKey(key, id, "bot-1") {
		t.Fatal("MatchKey() = false, ожидалось true")
	}
}

func TestMatchKey_WrongString(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

	key, err := EncodeKey(id, "bot-1")
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if MatchKey(key, id, "bot-2") {
		t.Fatal("MatchKey() = true, ожидалось false")
	}
}

func TestMatchKey_WrongUUID(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
	id2 := uuid.MustParse("b2c3d4e5-f6a7-8901-bcde-f12345678901")

	key, err := EncodeKey(id1, "bot-1")
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if MatchKey(key, id2, "bot-1") {
		t.Fatal("MatchKey() = true, ожидалось false")
	}
}

func TestMatchKey_NilKey(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

	if MatchKey(nil, id, "bot-1") {
		t.Fatal("MatchKey(nil) = true, ожидалось false")
	}
}

func TestMatchKey_EmptyKey(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

	if MatchKey([]byte{}, id, "bot-1") {
		t.Fatal("MatchKey([]byte{}) = true, ожидалось false")
	}
}

func TestMatchKey_UnsupportedTypeInParts(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(uuid.New(), "bot")
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if MatchKey(key, uuid.New(), 123) {
		t.Fatal("MatchKey() с int = true, ожидалось false")
	}
}

func TestMatchKey_MultiPartExact(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
	id2 := uuid.MustParse("b2c3d4e5-f6a7-8901-bcde-f12345678901")

	key, err := EncodeKey(id1, id2, "action", int64(7), true)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if !MatchKey(key, id1, id2, "action", int64(7), true) {
		t.Fatal("MatchKey() 5 parts = false, ожидалось true")
	}
}

func TestMatchKey_MultiPartWrongLast(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
	id2 := uuid.MustParse("b2c3d4e5-f6a7-8901-bcde-f12345678901")

	key, err := EncodeKey(id1, id2, "action", int64(7), true)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if MatchKey(key, id1, id2, "action", int64(7), false) {
		t.Fatal("MatchKey() с false = true, ожидалось false")
	}
}

func TestEncodeKey_Deterministic(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
	s := "hello"
	n := int64(99)

	key1, err := EncodeKey(id, s, n)
	if err != nil {
		t.Fatalf("EncodeKey() 1 вернул ошибку: %v", err)
	}

	key2, err := EncodeKey(id, s, n)
	if err != nil {
		t.Fatalf("EncodeKey() 2 вернул ошибку: %v", err)
	}

	if len(key1) != len(key2) {
		t.Fatalf("длины не совпадают: %d vs %d", len(key1), len(key2))
	}

	for i := range key1 {
		if key1[i] != key2[i] {
			t.Fatalf("байт %d не совпадает: %d vs %d", i, key1[i], key2[i])
		}
	}
}

func TestMatchKey_Int64Exact(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(int64(1234567890123))
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if !MatchKey(key, int64(1234567890123)) {
		t.Fatal("MatchKey() int64 = false, ожидалось true")
	}
}

func TestMatchKey_Int64Mismatch(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(int64(42))
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if MatchKey(key, int64(43)) {
		t.Fatal("MatchKey() int64 mismatch = true, ожидалось false")
	}
}

func TestMatchKey_BoolExact(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(true)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if !MatchKey(key, true) {
		t.Fatal("MatchKey() true = false, ожидалось true")
	}
}

func TestMatchKey_BoolMismatch(t *testing.T) {
	t.Parallel()

	key, err := EncodeKey(true)
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if MatchKey(key, false) {
		t.Fatal("MatchKey() true vs false = true, ожидалось false")
	}
}

func TestValidateKeyLength_Exact(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

	key, err := EncodeKey(id, "bot-1")
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	if err := ValidateKeyLength(key, id, "bot-1"); err != nil {
		t.Fatalf("ValidateKeyLength() вернул ошибку: %v", err)
	}
}

func TestValidateKeyLength_Longer(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

	key, err := EncodeKey(id, "bot-1")
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	// key длиннее ожидаемого для одного uuid — не короче минимума, поэтому не ошибка.
	if err := ValidateKeyLength(key, id); err != nil {
		t.Fatalf("ValidateKeyLength() с более длинным key вернул ошибку: %v", err)
	}
}

func TestValidateKeyLength_TooShort(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

	key, err := EncodeKey(id, "bot-1")
	if err != nil {
		t.Fatalf("EncodeKey() вернул ошибку: %v", err)
	}

	err = ValidateKeyLength(key[:len(key)-1], id, "bot-1")
	if err == nil {
		t.Fatal("ValidateKeyLength() вернул nil, ожидалась ошибка")
	}

	if !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("errors.Is(err, ErrInvalidKey) = false, err: %v", err)
	}
}

func TestValidateKeyLength_NilKey(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

	err := ValidateKeyLength(nil, id, "bot-1")
	if !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("errors.Is(err, ErrInvalidKey) = false, err: %v", err)
	}
}

func TestValidateKeyLength_EmptyPartsNilKey(t *testing.T) {
	t.Parallel()

	// Без parts ожидаемая длина 0 — любой (в т.ч. nil) key проходит.
	if err := ValidateKeyLength(nil); err != nil {
		t.Fatalf("ValidateKeyLength() без parts вернул ошибку: %v", err)
	}
}

func TestValidateKeyLength_UnsupportedType(t *testing.T) {
	t.Parallel()

	err := ValidateKeyLength([]byte{1, 2, 3}, 42) // int — не поддерживается
	if err == nil {
		t.Fatal("ValidateKeyLength() вернул nil, ожидалась ошибка")
	}

	if errors.Is(err, ErrInvalidKey) {
		t.Fatal("errors.Is(err, ErrInvalidKey) = true, ожидалось false (ошибка кодирования, не длины)")
	}
}
