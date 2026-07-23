package kafkax

import "slices"

// ConsumerMiddleware is a function that wraps a ConsumerHandler.
type ConsumerMiddleware func(ConsumerHandler) ConsumerHandler

// Chain applies middlewares to a ConsumerHandler in reverse order.
func Chain(handler ConsumerHandler, mws ...ConsumerMiddleware) ConsumerHandler {
	for _, v := range slices.Backward(mws) {
		handler = v(handler)
	}

	return handler
}
