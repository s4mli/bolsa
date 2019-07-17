package snsqs

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/sirupsen/logrus"
)

type consumerWorker struct {
	ctx      aws.Context
	cancelFn context.CancelFunc
	id       int
	handler  MessageHandler
	logger   logrus.FieldLogger
}

func (w *consumerWorker) run(c Consumer) chan struct{} {
	stopped := make(chan struct{})
	handle := func(sqsMsgAll []*sqs.Message) {
		var wg sync.WaitGroup
		wg.Add(len(sqsMsgAll))
		handleOne := func(sqsMsg *sqs.Message) {
			defer wg.Done()
			var snsMsg SNSMessage
			if err := json.Unmarshal([]byte(*sqsMsg.Body), &snsMsg); err != nil {
				w.logger.WithFields(logrus.Fields{
					"&": "run@Unmarshal",
					"*": *sqsMsg.Body,
				}).Error(err)
			} else {
				if err := w.handler.handle(&snsMsg); err == nil {
					if err := c.ack(w.ctx, sqsMsg); err != nil {
						w.logger.WithField("&", "run@ack").Error(err)
					}
				}
			}
		}
		for _, sqsMsg := range sqsMsgAll {
			go handleOne(sqsMsg)
		}
		wg.Wait()
	}

	go func() {
		for {
			select {
			case <-w.ctx.Done():
				w.logger.WithField("&", "run").Warn("!")
				stopped <- struct{}{}
				return
			default:
				if messages, err := c.consume(w.ctx); err != nil {
					w.logger.WithField("&", "run@consume").Error(err)
				} else {
					handle(messages)
				}
			}
		}
	}()
	return stopped
}

func newConsumerWorker(
	parentContext aws.Context,
	id int,
	name string,
	handler MessageHandler,
	logger logrus.FieldLogger,
) *consumerWorker {
	ctx, cancelFn := context.WithCancel(parentContext)
	return &consumerWorker{
		ctx:      ctx,
		cancelFn: cancelFn,
		id:       id,
		handler:  handler,
		logger:   logger.WithField("#", fmt.Sprintf("SQS:C(%s,%d)", name, id)),
	}
}
