package snsqs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/sirupsen/logrus"
)

type publisherWorker struct {
	ctx      aws.Context
	cancelFn context.CancelFunc
	id       int
	logger   logrus.FieldLogger
}

func (w *publisherWorker) run(p Publisher, inputCh chan content, onPublished OnPublished) chan struct{} {
	stopped := make(chan struct{})
	go func(inputCh <-chan content) {
		for {
			select {
			case <-w.ctx.Done():
				w.logger.WithField("&", "run").Warn("!")
				stopped <- struct{}{}
				return
			case d := <-inputCh:
				bodyInLog := strings.Replace(string(d.message), "\"", "", -1)
				d.messageId, d.failed = p.publish(w.ctx, d.message)
				if d.failed != nil {
					w.logger.WithFields(logrus.Fields{
						"&": "run@publish",
						"*": bodyInLog,
					}).Error(d.failed)
				} else {
					w.logger.WithFields(logrus.Fields{
						"&": "run@publish",
						"*": bodyInLog,
					}).Info(d.messageId)
				}
				if onPublished != nil {
					onPublished(d.message, d.messageId, d.failed)
				}
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}
	}(inputCh)
	return stopped
}

func newPublisherWorker(
	parentContext aws.Context,
	id int,
	name string,
	logger logrus.FieldLogger,
) *publisherWorker {
	ctx, cancelFn := context.WithCancel(parentContext)
	return &publisherWorker{
		ctx:      ctx,
		cancelFn: cancelFn,
		id:       id,
		logger:   logger.WithField("@", fmt.Sprintf("SNS:P(%s,%d)", name, id)),
	}
}
