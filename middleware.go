package kafkax

import "context"

type ConsumerHandler interface {
	ProcessMessage(ctx context.Context, msg IncomingMessage) error
}

type ConsumerHandlerFunc func(context.Context, IncomingMessage) error

func (f ConsumerHandlerFunc) ProcessMessage(ctx context.Context, msg IncomingMessage) error {
	return f(ctx, msg)
}

type ConsumerMiddleware func(ConsumerHandler) ConsumerHandler

func Chain(handler ConsumerHandler, mws ...ConsumerMiddleware) ConsumerHandler {
	for i := len(mws) - 1; i >= 0; i-- {
		handler = mws[i](handler)
	}
	return handler
}
