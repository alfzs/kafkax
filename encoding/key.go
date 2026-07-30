// Package encoding собирает и сравнивает бинарные композитные ключи Kafka.
//
// Пакет намеренно оставлен листом: он не импортирует ни kafkax, ни franz-go и
// зависит только от github.com/google/uuid. Тому, кому нужен лишь формат ключа
// — продюсеру на другой библиотеке, миграционному скрипту, тесту — не
// приходится тянуть за собой транспорт и телеметрию. Обратное направление
// (kafkax импортирует encoding) реализовано в kafkax.MatchKeyMiddleware.
package encoding

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/google/uuid"
)

// Размеры частей в байтах. Формат — часть внешнего контракта: по этим ключам
// Kafka партиционирует уже отправленные сообщения, и их читают продюсеры на
// других языках. Менять нельзя.
const (
	uuidSize      = 16
	lenPrefixSize = 4
	int64Size     = 8
	boolSize      = 1
)

var (
	// ErrInvalidKey — ключ короче размера, ожидаемого для заданных частей.
	ErrInvalidKey = errors.New("encoding: invalid composite key")

	// ErrInvalidKeyPart — часть не собрана ни одним из конструкторов
	// (UUID, Str, Int64, Bool): это нулевое значение KeyPart, например из
	// make([]KeyPart, n) или из незаполненного поля структуры.
	ErrInvalidKeyPart = errors.New("encoding: invalid key part")

	// ErrKeyPartTooLong — длина строковой части не помещается в uint32
	// префикса длины.
	ErrKeyPartTooLong = errors.New("encoding: key part too long")
)

// partKind различает варианты KeyPart. Нулевое значение — partInvalid: часть,
// собранная не конструктором, обязана быть отвергнута, а не закодирована как
// пустая.
type partKind uint8

const (
	partInvalid partKind = iota
	partUUID
	partString
	partInt64
	partBool
)

// KeyPart — одна часть композитного ключа.
//
// Значения собираются только конструкторами UUID, Str, Int64 и Bool, поэтому
// «неподдерживаемый тип части» перестал быть runtime-ошибкой: EncodeKey(42)
// больше не компилируется. Раньше части принимались как ...any, и опечатка в
// типе (int вместо int64) доезжала до прода, где лечилась паникой.
//
// Дженерик-параметр здесь не подходит: variadic-дженерик требует одного типа
// на все аргументы, а ключ по определению разнородный.
//
// Нулевое значение невалидно — см. ErrInvalidKeyPart.
type KeyPart struct {
	kind partKind
	flag byte // готовый байт partBool: конверсия int64 → byte при записи не нужна
	id   uuid.UUID
	str  string
	num  int64
}

// UUID — часть-UUID: 16 байт как есть, без префикса длины.
func UUID(v uuid.UUID) KeyPart {
	return KeyPart{kind: partUUID, id: v}
}

// Str — строковая часть: 4 байта длины big-endian, затем сами байты строки.
// Префикс длины делает кодировку самодостаточной: границы частей
// восстанавливаются без внешней информации о типах.
func Str(v string) KeyPart {
	return KeyPart{kind: partString, str: v}
}

// Int64 — часть-int64: 8 байт big-endian. Отрицательные значения кодируются
// реинтерпретацией битов в uint64, а не как sign-magnitude.
func Int64(v int64) KeyPart {
	return KeyPart{kind: partInt64, num: v}
}

// Bool — часть-bool: один байт, 1 или 0.
func Bool(v bool) KeyPart {
	var flag byte
	if v {
		flag = 1
	}

	return KeyPart{kind: partBool, flag: flag}
}

// size возвращает длину закодированной части и заодно проверяет её валидность:
// EncodeKey считает размер первым проходом, поэтому все отказы обязаны
// случиться здесь, до записи хотя бы одного байта в буфер.
func (p KeyPart) size(pos int) (int, error) {
	switch p.kind {
	case partUUID:
		return uuidSize, nil

	case partString:
		// uint64(len(p.str)) > math.MaxUint32, а не len(p.str) > 1<<32-1: на
		// 32-битных платформах правая часть — нетипизированная константа,
		// не влезающая в int, и файл просто не компилируется.
		if uint64(len(p.str)) > math.MaxUint32 {
			return 0, fmt.Errorf("%w: part %d (string): length %d exceeds max uint32",
				ErrKeyPartTooLong, pos, len(p.str))
		}

		return lenPrefixSize + len(p.str), nil

	case partInt64:
		return int64Size, nil

	case partBool:
		return boolSize, nil

	case partInvalid:
		return 0, fmt.Errorf("%w at position %d: zero-value KeyPart; "+
			"use encoding.UUID, Str, Int64 or Bool", ErrInvalidKeyPart, pos)

	default:
		return 0, fmt.Errorf("%w at position %d: unknown kind %d", ErrInvalidKeyPart, pos, p.kind)
	}
}

// appendTo дописывает часть в конец buf. Вызывается только после успешного
// size, поэтому невалидные варианты молча возвращают buf: собственная
// диагностика здесь дублировала бы size и была бы недостижима.
func (p KeyPart) appendTo(buf []byte) []byte {
	switch p.kind {
	case partUUID:
		return append(buf, p.id[:]...)

	case partString:
		//nolint:gosec // длина строки проверена в size() до вызова appendTo
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(p.str)))

		return append(buf, p.str...)

	case partInt64:
		//nolint:gosec // намеренная реинтерпретация битов для big-endian кодировки
		return binary.BigEndian.AppendUint64(buf, uint64(p.num))

	case partBool:
		return append(buf, p.flag)

	case partInvalid:
		return buf

	default:
		return buf
	}
}

// EncodeKey собирает бинарный композитный ключ из N упорядоченных частей.
//
// Каждая кодировка самодостаточна: границы частей восстанавливаются без
// внешней информации о типах — достаточно знать размер фиксированных
// типов и length-prefix для string.
//
// Итоговый размер считается первым проходом, запись идёт в один буфер: одна
// аллокация на ключ независимо от числа частей.
//
// Продюсер:
//
//	Key: encoding.EncodeKey(encoding.UUID(dto.TenantID), encoding.Str(dto.ExternalBotID))
//
// Ошибка возможна только на невалидной части (ErrInvalidKeyPart — нулевое
// значение KeyPart) или на строке длиннее math.MaxUint32 (ErrKeyPartTooLong).
func EncodeKey(parts ...KeyPart) ([]byte, error) {
	size := 0

	for i, part := range parts {
		partSize, err := part.size(i)
		if err != nil {
			return nil, err
		}

		size += partSize
	}

	buf := make([]byte, 0, size)
	for _, part := range parts {
		buf = part.appendTo(buf)
	}

	return buf, nil
}

// Key — предкодированный композитный ключ, готовый к многократному сравнению.
//
// Смысл типа в том, чтобы кодирование частей случилось один раз, а не на
// каждое сообщение: kafkax.MatchKeyMiddleware строит Key при сборке цепочки и
// на сообщение делает только len и bytes.Equal. Проверка длины и сравнение
// живут здесь в единственном экземпляре — MatchKey, ValidateKeyLength и
// middleware зовут именно их.
//
// Нулевое значение Key соответствует пустому ключу и осмысленно: NewKey без
// частей даёт ровно его.
type Key struct {
	raw []byte
}

// NewKey кодирует части один раз и возвращает Key для последующих сравнений.
func NewKey(parts ...KeyPart) (Key, error) {
	raw, err := EncodeKey(parts...)
	if err != nil {
		return Key{}, err
	}

	return Key{raw: raw}, nil
}

// Bytes возвращает байты ключа — например, чтобы отправить сообщение с тем же
// ключом, по которому фильтруется консьюмер.
//
// Срез внутренний: мутация испортит все последующие сравнения, поэтому его
// нужно только читать или копировать.
func (k Key) Bytes() []byte {
	return k.raw
}

// Match сообщает, совпадает ли key с предкодированным ключом побайтово.
// Decode не нужен: консьюмер знает свои значения и сравнивает кодировки.
func (k Key) Match(key []byte) bool {
	return bytes.Equal(key, k.raw)
}

// ValidateLength возвращает ErrInvalidKey, если key короче предкодированного —
// например, ключ Kafka-сообщения усечён или это ключ чужого формата.
//
// Ключ длиннее ожидаемого невалидным не считается: это просто не то, что дали
// бы эти части, и обнаруживается сравнением в Match.
func (k Key) ValidateLength(key []byte) error {
	if len(key) < len(k.raw) {
		return fmt.Errorf("%w: got %d bytes, want at least %d", ErrInvalidKey, len(key), len(k.raw))
	}

	return nil
}

// MatchKey кодирует parts в ключ и сравнивает с key побайтово.
//
// Паникует, если кодирование отказало (нулевое значение KeyPart, строка длиннее
// uint32). Набор parts статичен в коде вызывающего и от данных не зависит,
// поэтому отказ здесь — ошибка программиста, а не свойство сообщения. Тихий
// false превращал бы такую ошибку в «ключ не наш»: обработчик возвращал бы nil,
// оффсет коммитился, и весь трафик отбрасывался при зелёных метриках успеха.
// Паника, случившаяся внутри обработчика, перехватывается consumer'ом,
// логируется со стеком и учитывается метрикой kafkax.consumer.panics.
//
// Кодирует parts на каждый вызов. Если сравнение идёт на каждое сообщение —
// вместо MatchKey нужен NewKey один раз и Key.Match дальше; готовая связка
// «проверить длину, затем сравнить» — kafkax.MatchKeyMiddleware.
func MatchKey(key []byte, parts ...KeyPart) bool {
	want, err := NewKey(parts...)
	if err != nil {
		panic(fmt.Sprintf("kafkax/encoding: MatchKey: %v", err))
	}

	return want.Match(key)
}

// ValidateKeyLength возвращает ErrInvalidKey, если key короче длины, которую
// дало бы EncodeKey(parts...).
//
// Обёртка над Key.ValidateLength для разовых проверок: parts кодируются на
// каждый вызов. На горячем пути нужен NewKey один раз и Key.ValidateLength
// дальше — так делает kafkax.MatchKeyMiddleware.
//
// Полезно там, где важно отличить «ключ другого тенанта» (MatchKey вернёт
// false для валидного по длине ключа) от «сообщение повреждено».
func ValidateKeyLength(key []byte, parts ...KeyPart) error {
	want, err := NewKey(parts...)
	if err != nil {
		return err
	}

	return want.ValidateLength(key)
}
