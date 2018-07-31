package sqs

import (
	"context"

	"github.com/samwooo/bolsa/pubsub"
)

/////////////////////
// SQS Subscriber //
type Subscriber struct {
	q *Queue
	*pubsub.SubJob
}

func (s *Subscriber) Subscribe(ctx context.Context, handle pubsub.MessageHandler) {
	if s.SubJob == nil {
		s.SubJob = pubsub.NewSubJob(ctx, s.q.Logger, s.q, handle)
	}
	s.SubJob.Run()
}

func NewSubscriber(ctx context.Context, q *Queue) *Subscriber {
	return &Subscriber{q, nil}
}
