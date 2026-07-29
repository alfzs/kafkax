package kafkax

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	"github.com/alfzs/kafkax/v2/encoding"
)

// ConsumerMiddleware оборачивает ConsumerHandler.
type ConsumerMiddleware func(ConsumerHandler) ConsumerHandler

// Chain применяет middleware к ConsumerHandler в обратном порядке, так что
// первый переданный mws оказывается внешним.
func Chain(handler ConsumerHandler, mws ...ConsumerMiddleware) ConsumerHandler {
	for _, v := range slices.Backward(mws) {
		handler = v(handler)
	}

	return handler
}

// MatchKeyMiddleware пропускает к обработчику только те сообщения, ключ которых
// побайтово равен encoding.EncodeKey(parts...) — например, свой тенант в топике,
// который читают все:
//
//	kafkax.Chain(handler, kafkax.MatchKeyMiddleware(tenantID, externalBotID))
//
// Чужой ключ — не ошибка: обработчик не вызывается, middleware возвращает nil,
// оффсет отмечается, чтение идёт дальше.
//
// Ключ короче ожидаемого — ошибка encoding.ErrInvalidKey. Это не «сообщение
// другого тенанта», а повреждённое или чужого формата сообщение, и молча
// коммитить его нельзя: по умолчанию оно остановит партицию (см. раздел
// «Политика повторов»). Если в топике штатно соседствуют ключи разных схем,
// такую ошибку нужно гасить своим Config.OnMessageSkipped.
//
// parts кодируются один раз, при сборке цепочки, а не на каждом сообщении.
// Отсюда паника при неподдерживаемом типе части: набор parts статичен в коде
// вызывающего, поэтому ошибка здесь — ошибка программиста, и обнаружиться она
// должна на старте процесса, а не на первом сообщении (см. encoding.MatchKey).
// Вызов без частей — тоже паника: encoding.EncodeKey() вернул бы пустой ключ,
// и middleware молча отбросил бы весь трафик топика. Это отказ вида «метрики
// зелёные, обработано ноль сообщений» — ровно тот, против которого здесь стоят
// остальные паники.
func MatchKeyMiddleware(parts ...any) ConsumerMiddleware {
	if len(parts) == 0 {
		panic("kafkax: MatchKeyMiddleware: no key parts given; " +
			"an empty key would silently drop every message")
	}

	want, err := encoding.EncodeKey(parts...)
	if err != nil {
		panic(fmt.Sprintf("kafkax: MatchKeyMiddleware: %v", err))
	}

	return func(next ConsumerHandler) ConsumerHandler {
		return ConsumerHandlerFunc(func(ctx context.Context, msg IncomingMessage) error {
			if len(msg.Key) < len(want) {
				return fmt.Errorf("match key middleware: %w: got %d bytes, want at least %d",
					encoding.ErrInvalidKey, len(msg.Key), len(want))
			}

			if !bytes.Equal(msg.Key, want) {
				return nil
			}

			return next.ProcessMessage(ctx, msg)
		})
	}
}
