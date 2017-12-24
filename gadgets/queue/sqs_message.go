package queue

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type sqsMessage struct {
	queue         *SqsQueue
	messageId     string
	receiptHandle string
	payload       []byte
}

func (m *sqsMessage) Payload() []byte {
	return m.payload
}

func (m *sqsMessage) ChangeVisibility(remainingInvisibility time.Duration) error {
	params := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(m.queue.url),
		ReceiptHandle:     aws.String(m.receiptHandle),
		VisibilityTimeout: aws.Int64(int64(remainingInvisibility / time.Second)),
	}
	if resp, err := m.queue.svc.ChangeMessageVisibility(params); err != nil {
		m.queue.logger.Errf("ChangeMessageVisibility err: %s", err.Error())
		return err
	} else {
		m.queue.logger.Debugf("ChangeMessageVisibility done: %+v.", resp)
		return nil
	}
}

func (m *sqsMessage) Delete() error {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(m.queue.url),
		ReceiptHandle: aws.String(m.receiptHandle),
	}
	if resp, err := m.queue.svc.DeleteMessage(params); err != nil {
		m.queue.logger.Errf("DeleteMessage err: %s", err.Error())
		return err
	} else {
		m.queue.logger.Debugf("DeleteMessage done: %+v.", resp)
		return nil
	}
}
