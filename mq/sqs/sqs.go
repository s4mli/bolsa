package sqs

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/job/model"
	"github.com/samwooo/bolsa/logging"
)

func connect(logger logging.Logger, qRegion string) *sqs.SQS {
	config := &aws.Config{
		Region:   &qRegion,
		LogLevel: aws.LogLevel(aws.LogOff)}
	var err error = nil
	var s *session.Session = nil
	retry := 0
	for {
		if s, err = session.NewSession(config); err != nil {
			logger.Errorf("connect failed ( %d, %s )", retry, err.Error())
			retry++
			time.Sleep(common.RandomDuration(retry))
		} else {
			break
		}
	}
	return sqs.New(s, config)
}

func ack(logger logging.Logger, qService *sqs.SQS, qUrl string, msg *sqs.Message) error {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(qUrl),
		ReceiptHandle: msg.ReceiptHandle,
	}
	if _, err := qService.DeleteMessage(params); err != nil {
		logger.Errorf("ack ( %s ) failed ( %s )", *msg.Body, err.Error())
		return err
	} else {
		return nil
	}
}

func poll(ctx context.Context, logger logging.Logger, qService *sqs.SQS, qUrl string, qWait int64) (
	*sqs.Message, error) {
	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(qUrl),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(qWait),
	}
	if resp, err := qService.ReceiveMessageWithContext(ctx, params); err != nil {
		logger.Errorf("poll failed ( %s )", err.Error())
		return nil, err
	} else {
		if len(resp.Messages) == 1 {
			return resp.Messages[0], nil
		}
		err := fmt.Errorf("( %d ) messages retrieved", len(resp.Messages))
		logger.Error(err)
		return nil, err
	}
}

type MessageHandler func(string) error

func (mh MessageHandler) handle(body string) error { return mh(body) }

func consume(ctx context.Context, logger logging.Logger, qService *sqs.SQS, qUrl string, qWait int64,
	handler MessageHandler) error {
	if msg, err := poll(ctx, logger, qService, qUrl, qWait); err != nil {
		return err
	} else {
		if err := handler.handle(*msg.Body); err != nil {
			logger.Errorf("handle message ( %s ) failed ( %s )", *msg.Body, err.Error())
			ack(logger, qService, qUrl, msg)
			return err
		} else {
			logger.Debugf("handle message ( %s ) succeed", *msg.Body)
			return ack(logger, qService, qUrl, msg)
		}
	}
}

func retrieve(ctx context.Context, logger logging.Logger, qService *sqs.SQS, qUrl string, qWait int64,
	labor model.Labor) error {
	if msg, err := poll(ctx, logger, qService, qUrl, qWait); err != nil {
		return err
	} else {
		if r, err := labor.Work(*msg.Body); err != nil {
			logger.Errorf("handle message ( %s ) failed ( %s )", *msg.Body, err.Error())
			ack(logger, qService, qUrl, msg)
			return err
		} else {
			logger.Debugf("handle message ( %s ) succeed ( %+v )", *msg.Body, r)
			return ack(logger, qService, qUrl, msg)
		}
	}
}
