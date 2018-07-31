package sqs

import (
	"context"
	"fmt"
	"strings"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/job/model"
	"github.com/samwooo/bolsa/pubsub"
)

////////////////////
// SQS Publisher //
type Publisher struct {
	q         *Queue
	batchSize int
	maxRetry  int
	*pubsub.PubJob
}

func (p *Publisher) PublishAsync(ctx context.Context, messageBody []interface{}) {
	if p.PubJob == nil {
		p.PubJob = pubsub.NewPubJob(ctx, p.q.Logger, p.q, messageBody, p.batchSize, p.maxRetry, true)
		p.PubJob.Run()
	} else {
		p.PubJob.Push(messageBody)
	}
}

func (p *Publisher) Publish(ctx context.Context, messageBody []interface{}) ([]pubsub.MessageId, error) {
	r := pubsub.NewPubJob(ctx, p.q.Logger, p.q, messageBody, p.batchSize, p.maxRetry, false).Run()
	var messageIds []pubsub.MessageId
	var errs []string
	r.Range(func(key, value interface{}) bool {
		if v, ok := value.(model.Done); ok {
			if id, isId := v.R.(pubsub.MessageId); isId {
				messageIds = append(messageIds, id)
			} else {
				errs = append(errs, fmt.Sprintf("cast %+v to MessageId falied", v.R))
			}
		} else {
			errs = append(errs, fmt.Sprintf("cast %+v to Done falied", value))
		}
		return true
	})
	return messageIds, common.ErrorFromString(strings.Join(errs, " | "))
}

func NewPublisher(ctx context.Context, q *Queue, batchSize, maxRetry int) *Publisher {
	return &Publisher{q, batchSize, maxRetry, nil}
}
