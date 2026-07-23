package encoding

import (
	"context"
	"fmt"

	"github.com/alfzs/kafkax"
	"github.com/google/uuid"
)

// MatchKeyMiddleware returns a ConsumerMiddleware that discards messages
// whose composite key does not match myTenantID + myExternalBotID.
//
// A key shorter than the encoded myTenantID+myExternalBotID is treated as
// malformed (ErrInvalidKey) rather than "different tenant" — it's returned
// as an error so the consumer's retry/logging path surfaces it instead of
// silently dropping a corrupted message.
func MatchKeyMiddleware(myTenantID uuid.UUID, myExternalBotID string) kafkax.ConsumerMiddleware {
	return func(next kafkax.ConsumerHandler) kafkax.ConsumerHandler {
		return kafkax.ConsumerHandlerFunc(func(ctx context.Context, msg kafkax.IncomingMessage) error {
			if err := ValidateKeyLength(msg.Key, myTenantID, myExternalBotID); err != nil {
				return fmt.Errorf("match key middleware: %w", err)
			}

			if !MatchKey(msg.Key, myTenantID, myExternalBotID) {
				return nil
			}

			return next.ProcessMessage(ctx, msg)
		})
	}
}
