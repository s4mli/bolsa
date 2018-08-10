package pubsub

import "context"

type MessageId string
type Broker interface {
	// push any amount of message body, return only one messageId once
	Push(ctx context.Context, messageBody []interface{}) (MessageId, error)
	// poll only one message with batched amount of message body once
	Poll(ctx context.Context) (messageBody []interface{}, err error)
	Len() (uint64, error)
}

type Publisher interface {
	// publish any amount of message body to Broker, return multiple messageId due to batching
	Publish(ctx context.Context, messageBody []interface{}) ([]MessageId, error)
	PublishAsync(ctx context.Context, messageBody []interface{})
}

type MessageHandler func(messageBody []interface{}) error

func (mh MessageHandler) handle(messageBody []interface{}) error { return mh(messageBody) }

type Subscriber interface {
	// subscribe to Broker, handle batched amount of message body per message
	Subscribe(ctx context.Context, handler MessageHandler)
}
