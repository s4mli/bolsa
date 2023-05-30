package snsqs

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/s4mli/bolsa/cleaner"
	"github.com/sirupsen/logrus"
)

type content struct {
	message   []byte
	messageId string
	failed    error
}

type publisher struct {
	*sns.SNS
	cancelFn  context.CancelFunc
	topicArn  string
	topicName string
	inputCh   chan content
	workers   map[int]*publisherWorker
	stopped   []chan struct{}
}

func (p *publisher) publish(ctx aws.Context, message []byte) (string, error) {
	if output, err := p.PublishWithContext(ctx, &sns.PublishInput{
		Message:  aws.String(string(message)),
		TopicArn: aws.String(p.topicArn),
	}); err != nil {
		return "", err
	} else {
		return *output.MessageId, nil
	}
}

func (p *publisher) Name() string { return "SNS:P" }
func (p *publisher) Stop() {
	p.cancelFn()
	for _, stopped := range p.stopped {
		<-stopped
	}
}

func (p *publisher) PublishJson(message interface{}) error {
	if jsonMessage, err := json.Marshal(message); err != nil {
		return err
	} else {
		p.inputCh <- content{message: jsonMessage}
		return nil
	}
}

func (p *publisher) Run(published OnPublished) {
	for _, v := range p.workers {
		p.stopped = append(p.stopped, v.run(p, p.inputCh, published))
	}
}

func newPublisher(
	parentCtx aws.Context,
	region, topicArn, topicName string,
	count int,
	logger logrus.FieldLogger,
) Publisher {
	ctx, cancelFn := context.WithCancel(parentCtx)
	p := &publisher{
		SNS: sns.New(session.Must(session.NewSession(&aws.Config{
			Region:   aws.String(region),
			LogLevel: aws.LogLevel(aws.LogOff),
		}))),
		cancelFn:  cancelFn,
		topicArn:  topicArn,
		topicName: topicName,
		inputCh:   make(chan content, count),
		workers: func() map[int]*publisherWorker {
			workers := make(map[int]*publisherWorker, count)
			for id := 0; id < count; id++ {
				workers[id] = newPublisherWorker(ctx, id, topicName, logger)
			}
			return workers
		}(),
	}
	cleaner.Register(p)
	return p
}
