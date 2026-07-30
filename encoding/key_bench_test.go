package encoding

import "testing"

// Бенчмарки RF-PERF-05. До двухпроходной сборки (рост буфера из nil плюс по
// временному make на строку и на int64) каждый из четырёх давал 4 allocs/op:
//
//	EncodeKey_2UUID           ~119 ns   80 B   4 allocs
//	EncodeKey_UUIDStringInt   ~150 ns  128 B   4 allocs
//	MatchKey_2UUID            ~130 ns   80 B   4 allocs
//	ValidateKeyLength_2UUID   ~125 ns   80 B   4 allocs
//
// На горячем пути kafkax этого не было и нет: MatchKeyMiddleware кодирует части
// один раз при сборке цепочки. Бенчмарки сторожат путь пользователя, который
// зовёт EncodeKey или MatchKey на каждое сообщение.

func BenchmarkEncodeKey_2UUID(b *testing.B) {
	for b.Loop() {
		key, err := EncodeKey(UUID(testID1), UUID(testID2))
		if err != nil {
			b.Fatal(err)
		}

		_ = key
	}
}

func BenchmarkEncodeKey_UUIDStringInt(b *testing.B) {
	for b.Loop() {
		key, err := EncodeKey(UUID(testID1), Str(testBotID), Int64(7))
		if err != nil {
			b.Fatal(err)
		}

		_ = key
	}
}

func BenchmarkMatchKey_2UUID(b *testing.B) {
	key, err := EncodeKey(UUID(testID1), UUID(testID2))
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		if !MatchKey(key, UUID(testID1), UUID(testID2)) {
			b.Fatal("no match")
		}
	}
}

func BenchmarkValidateKeyLength_2UUID(b *testing.B) {
	key, err := EncodeKey(UUID(testID1), UUID(testID2))
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		if err := ValidateKeyLength(key, UUID(testID1), UUID(testID2)); err != nil {
			b.Fatal(err)
		}
	}
}

// Предкодированный Key — то, что стоит на горячем пути MatchKeyMiddleware:
// нулевые аллокации на сообщение независимо от числа частей.
func BenchmarkKeyMatch_2UUID(b *testing.B) {
	want, err := NewKey(UUID(testID1), UUID(testID2))
	if err != nil {
		b.Fatal(err)
	}

	key := want.Bytes()

	for b.Loop() {
		if err := want.ValidateLength(key); err != nil {
			b.Fatal(err)
		}

		if !want.Match(key) {
			b.Fatal("no match")
		}
	}
}
