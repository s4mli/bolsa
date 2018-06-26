package pubsub

import "context"

type EventId string

type Event interface {
	Id() EventId
	Ack() error
	// Parse EventBody
	Parse() ([]interface{}, error)
}

type Broker interface {
	// push any amount of EventBody as you wish
	Push(ctx context.Context, body []interface{}) ([]EventId, error)
	// Pop 1 EventBody once
	Pop(ctx context.Context) ([]interface{}, error)
	// Get 1 EventBody once
	Get(context.Context, EventId) ([]interface{}, error)
	Len() (uint64, error)
}

type Publisher interface {
	Publish(ctx context.Context, body []interface{}) ([]EventId, error)
}

type Subscriber interface {
	Subscribe(ctx context.Context) error
}

type Client interface {
	Publisher(Broker) (Publisher, error)
	Subscriber(Broker) (Subscriber, error)
	Pair(Broker) (Publisher, Subscriber, error)
}
