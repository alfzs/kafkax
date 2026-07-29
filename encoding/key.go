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

// EncodeKey собирает бинарный композитный ключ из N упорядоченных частей.
//
// Поддерживаемые типы:
//   - uuid.UUID (16 bytes)
//   - string (4 bytes big-endian length + data)
//   - int64 (8 bytes big-endian)
//   - bool (1 byte)
//
// Каждая кодировка самодостаточна: границы частей восстанавливаются без
// внешней информации о типах — достаточно знать размер фиксированных
// типов и length-prefix для string.
//
// Продюсер:
//
//	Key: encoding.EncodeKey(dto.TenantID, dto.ExternalBotID)
func EncodeKey(parts ...any) ([]byte, error) {
	var buf []byte

	for i, part := range parts {
		switch v := part.(type) {
		case uuid.UUID:
			buf = append(buf, v[:]...)

		case string:
			// uint64(len(v)) > math.MaxUint32, а не len(v) > 1<<32-1: на
			// 32-битных платформах правая часть — нетипизированная константа,
			// не влезающая в int, и файл просто не компилируется.
			if uint64(len(v)) > math.MaxUint32 {
				return nil, fmt.Errorf("encoding: part %d (string): length %d exceeds max uint32", i, len(v))
			}

			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, uint32(len(v))) //nolint:gosec // length already validated above
			buf = append(buf, lenBuf...)
			buf = append(buf, v...)

		case int64:
			intBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(intBuf, uint64(v)) //nolint:gosec // intentional reinterpretation for big-endian encoding
			buf = append(buf, intBuf...)

		case bool:
			if v {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}

		default:
			return nil, fmt.Errorf("encoding: unsupported key part type %T at position %d", part, i)
		}
	}

	return buf, nil
}

// MatchKey кодирует parts в ключ и сравнивает с key побайтово.
//
// Если key соответствует parts — совпадение. Decode ключа не нужен:
// консьюмер знает свои значения, кодирует их заново и сравнивает.
//
// Паникует, если EncodeKey отказал (неподдерживаемый тип части). Набор parts
// статичен в коде вызывающего и от данных не зависит, поэтому отказ здесь —
// ошибка программиста, а не свойство сообщения. Прежнее поведение — тихий
// false — превращало опечатку в типе (int вместо int64) в «ключ не наш»:
// обработчик возвращал nil, оффсет коммитился, и весь трафик отбрасывался при
// зелёных метриках успеха. Паника, случившаяся внутри обработчика,
// перехватывается consumer'ом, логируется со стеком и учитывается метрикой
// kafkax.consumer.panics.
//
// Готовая связка «проверить длину, затем сравнить» — kafkax.MatchKeyMiddleware;
// она кодирует parts один раз при сборке цепочки, поэтому неподдерживаемый тип
// роняет процесс на старте, а не на первом сообщении.
func MatchKey(key []byte, parts ...any) bool {
	encoded, err := EncodeKey(parts...)
	if err != nil {
		panic(fmt.Sprintf("kafkax/encoding: MatchKey: %v", err))
	}

	return bytes.Equal(key, encoded)
}

// ErrInvalidKey — ключ короче размера, ожидаемого для заданных parts.
var ErrInvalidKey = errors.New("encoding: invalid composite key")

// ValidateKeyLength возвращает ErrInvalidKey, если key короче длины,
// которую дало бы EncodeKey(parts...) — например, ключ Kafka-сообщения
// усечён или повреждён. Декодирование не выполняется: длина ожидаемого
// ключа вычисляется кодированием parts заново, как в MatchKey.
//
// Ключ длиннее ожидаемого не считается невалидным — это просто не то,
// что закодировали бы parts, и обнаруживается сравнением в MatchKey.
//
// Полезно там, где важно отличить «ключ другого тенанта» (MatchKey вернёт
// false для валидного по длине ключа) от «сообщение повреждено» — см.
// kafkax.MatchKeyMiddleware.
func ValidateKeyLength(key []byte, parts ...any) error {
	encoded, err := EncodeKey(parts...)
	if err != nil {
		return err
	}

	if len(key) < len(encoded) {
		return fmt.Errorf("%w: got %d bytes, want at least %d", ErrInvalidKey, len(key), len(encoded))
	}

	return nil
}
