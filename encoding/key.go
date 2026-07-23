package encoding

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

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
			if len(v) > 1<<32-1 {
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
//	func (h *handler) ProcessMessage(ctx context.Context, msg kafkax.IncomingMessage) error {
//	    if !encoding.MatchKey(msg.Key, h.myTenantID, h.myExternalBotID) {
//	        return nil
//	    }
//	    ...
//	}
//
// Ошибка кодирования (неподдерживаемый тип parts) считается несовпадением.
func MatchKey(key []byte, parts ...any) bool {
	encoded, err := EncodeKey(parts...)
	if err != nil {
		return false
	}

	return bytes.Equal(key, encoded)
}

// ErrInvalidKey — ключ короче минимального размера.
var ErrInvalidKey = errors.New("encoding: invalid composite key")
