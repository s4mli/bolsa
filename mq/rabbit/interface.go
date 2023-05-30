package rabbit

import (
	"golang.org/x/net/context"

	"github.com/s4mli/bolsa/cleaner"
)

type Medium interface {
	cleaner.Cleanable
	Context() context.Context
	Start()
	Notify([]chan struct{})
	Accept(string) (established, terminated, teared chan<- struct{})
	Await()
	Clean()
}

type Publisher interface {
	Publish(exchange, topic string, data []byte)
	PublishJson(exchange, topic string, data interface{}) error
}
