package encoding

import (
	"context"

	"github.com/alfzs/kafkax"
	"github.com/google/uuid"
)

func MatchKeyMiddleware(myTenantID uuid.UUID, myExternalBotID string) kafkax.ConsumerMiddleware {
	return func(next kafkax.ConsumerHandler) kafkax.ConsumerHandler {
		return kafkax.ConsumerHandlerFunc(func(ctx context.Context, msg kafkax.IncomingMessage) error {
			if !MatchKey(msg.Key, myTenantID, myExternalBotID) {
				return nil
			}
			return next.ProcessMessage(ctx, msg)
		})
	}
}
