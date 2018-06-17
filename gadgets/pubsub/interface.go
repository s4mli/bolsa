package pubsub

import "context"

type MessageId string

type Message interface {
	Id() MessageId
	Ack() error
	Parse() ([]interface{}, error)
}

type Broker interface {
	Put(ctx context.Context, body []interface{}) ([]MessageId, error)
	Pop(ctx context.Context) (Message, error)
	Len() (uint64, error)
}

type Publisher interface {
	Publish(ctx context.Context, body []interface{}, to Broker) ([]MessageId, error)
}

type Subscriber interface {
	Subscribe(ctx context.Context, from Broker) error
}

type Client interface {
	Publisher(Broker) (Publisher, error)
	Subscriber(Broker) (Publisher, error)
	Pair(Broker) (Publisher, Subscriber, error)
}
