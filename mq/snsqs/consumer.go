package snsqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/samwooo/bolsa/cleaner"
	"github.com/sirupsen/logrus"
)

// message from sns to sqs
type SNSMessage struct {
	Message        string `json:"Message"`
	TopicArn       string `json:"TopicArn"`
	MessageId      string `json:"MessageId"`
	UnsubscribeURL string `json:"UnsubscribeURL"`
}
type MessageHandler func(*SNSMessage) error

func (mh MessageHandler) handle(message *SNSMessage) error { return mh(message) }

type consumer struct {
	*sqs.SQS
	cancelFn            context.CancelFunc
	url, name           string
	waitTimeSeconds     int
	maxNumberOfMessages int
	workers             map[int]*consumerWorker
	stopped             []chan struct{}
}

func (c *consumer) consume(ctx aws.Context) ([]*sqs.Message, error) {
	if output, err := c.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(c.url),
		AttributeNames: []*string{
			aws.String("All"),
		},
		MaxNumberOfMessages: aws.Int64(int64(c.maxNumberOfMessages)),
		MessageAttributeNames: []*string{
			aws.String("All"),
		},
		WaitTimeSeconds: aws.Int64(int64(c.waitTimeSeconds)),
	}); err != nil {
		return nil, err
	} else {
		if len(output.Messages) > 0 {
			return output.Messages, nil
		} else {
			return nil, fmt.Errorf("empty q")
		}
	}
}
func (c *consumer) ack(ctx aws.Context, message *sqs.Message) error {
	if _, err := c.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.url),
		ReceiptHandle: message.ReceiptHandle,
	}); err != nil {
		return err
	} else {
		return nil
	}
}

func (c *consumer) Name() string { return "SQS:C" }
func (c *consumer) Stop() {
	c.cancelFn()
	for _, stopped := range c.stopped {
		<-stopped
	}
}

func (c *consumer) Run() {
	for _, v := range c.workers {
		c.stopped = append(c.stopped, v.run(c))
	}
}

func newConsumer(
	parentCtx aws.Context,
	region, url, name string,
	waitTimeSeconds, maxNumberOfMessages, count int,
	handler MessageHandler,
	logger logrus.FieldLogger,
) Consumer {
	ctx, cancelFn := context.WithCancel(parentCtx)
	c := &consumer{
		SQS: sqs.New(session.Must(session.NewSession(&aws.Config{
			Region:   aws.String(region),
			LogLevel: aws.LogLevel(aws.LogOff),
		}))),
		cancelFn:            cancelFn,
		url:                 url,
		name:                name,
		waitTimeSeconds:     waitTimeSeconds,
		maxNumberOfMessages: maxNumberOfMessages,
		workers: func() map[int]*consumerWorker {
			workers := make(map[int]*consumerWorker, count)
			for id := 0; id < count; id++ {
				workers[id] = newConsumerWorker(ctx, id, name, handler, logger)
			}
			return workers
		}(),
	}
	cleaner.Register(c)
	return c
}
