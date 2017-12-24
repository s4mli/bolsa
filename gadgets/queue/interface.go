package queue

import (
	"time"
)

type Message interface {
	Payload() []byte
	ChangeVisibility(remainingInvisibility time.Duration) error
	Delete() error
}

type Queue interface {
	SendMessage(delay int, payload []byte) error
	ReceiveMessage() (Message, error)
}
