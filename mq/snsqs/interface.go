package snsqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/samwooo/bolsa/cleaner"
	"github.com/sirupsen/logrus"
)

type Consumer interface {
	cleaner.Cleanable
	Run()
	consume(aws.Context) ([]*sqs.Message, error)
	ack(aws.Context, *sqs.Message) error
}

type OnPublished func(message []byte, messageId string, failed error)

type Publisher interface {
	cleaner.Cleanable
	Run(OnPublished)
	PublishJson(interface{}) error
	publish(aws.Context, []byte) (string, error)
}

/***********/
/* Facade */
/*********/
type Broker struct {
	region string
}

func (b *Broker) RunConsumer(
	ctx aws.Context,
	url, name string,
	waitTimeSeconds int,
	maxNumberOfMessages int,
	workers int,
	handler MessageHandler,
	logger logrus.FieldLogger) {
	newConsumer(ctx, b.region, url, name, waitTimeSeconds,
		maxNumberOfMessages, workers, handler, logger).Run()
}

func (b *Broker) RunPublisher(
	ctx aws.Context,
	topicArn, topicName string,
	workers int,
	published OnPublished,
	logger logrus.FieldLogger) Publisher {
	p := newPublisher(ctx, b.region, topicArn, topicName, workers, logger)
	p.Run(published)
	return p
}

func (b *Broker) RunPair(
	ctx aws.Context,
	topicArn, topicName string,
	publisherWorkers int,
	published OnPublished,
	queueUrl, queueName string,
	queueWaitTimeSeconds, queueMaxNumberOfMessages int,
	consumerWorkers int,
	handler MessageHandler,
	logger logrus.FieldLogger) Publisher {
	b.RunConsumer(ctx, queueUrl, queueName, queueWaitTimeSeconds,
		queueMaxNumberOfMessages, consumerWorkers, handler, logger)
	return b.RunPublisher(ctx, topicArn, topicName, publisherWorkers, published, logger)
}

func NewBroker(region string) *Broker {
	return &Broker{region}
}
