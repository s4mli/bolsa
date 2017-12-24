package queue

import (
	"encoding/base64"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/samwooo/bolsa/gadgets/config"
	"github.com/samwooo/bolsa/gadgets/logging"
)

type SqsQueue struct {
	svc    *sqs.SQS
	url    string
	wait   int
	logger logging.Logger
}

func encodePayload(payload []byte) string {
	return base64.StdEncoding.EncodeToString(payload)
}

func decodePayload(msgPayload string) ([]byte, error) {
	if data, err := base64.StdEncoding.DecodeString(msgPayload); err != nil {
		return nil, err
	} else {
		return data, nil
	}
}

func (s *SqsQueue) SendMessage(delay int, payload []byte) error {
	params := &sqs.SendMessageInput{
		MessageBody:  aws.String(encodePayload(payload)),
		QueueUrl:     aws.String(s.url),
		DelaySeconds: aws.Int64(int64(delay)),
	}
	s.logger.Debugf("SendMessage with: %+v", params)
	if resp, err := s.svc.SendMessage(params); err != nil {
		s.logger.Err(err)
		return err
	} else {
		s.logger.Debugf("SendMessage done: %+v", resp)
		return nil
	}
}

func (s *SqsQueue) ReceiveMessage() (Message, error) {
	// Poll SQS for a message.
	// Each call to the ReceiveMessage will block for at most s.waitTime.
	pollOneMessageUntil := func() (*sqs.Message, error) {
		params := &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(s.url),
			MaxNumberOfMessages: aws.Int64(1),
			WaitTimeSeconds:     aws.Int64(int64(s.wait)),
		}
		for i := 1; true; i++ {
			if resp, err := s.svc.ReceiveMessage(params); err != nil {
				return nil, err
			} else {
				if len(resp.Messages) == 1 {
					s.logger.Debugf("ReceiveMessage done: %+v", resp)
					return resp.Messages[0], nil
				}
				if len(resp.Messages) > 1 {
					return nil, fmt.Errorf("got %d( > 1 ) messages", len(resp.Messages))
				}
			}
		}
		return nil, fmt.Errorf("reached unreachable statement")
	}

	if awsMsg, err := pollOneMessageUntil(); err != nil {
		s.logger.Err(err)
		return nil, err
	} else {
		if payload, err := decodePayload(*awsMsg.Body); err != nil {
			s.logger.Err(err)
			return nil, err
		} else {
			s.logger.Debugf("DecodePayload done: %s", string(payload))
			msg := &sqsMessage{
				queue:         s,
				messageId:     *awsMsg.MessageId,
				receiptHandle: *awsMsg.ReceiptHandle,
				payload:       payload,
			}
			return msg, nil
		}
	}
}

// Create a new SQS queue instance bound to the specified url. waitTime is the number of seconds
// that an sqs.ReceiveMessage() should wait, at most, for a message to arrive. If it is set to a
// non-zero number then long-polling is enabled, as described here:
// http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
func NewSqsQueue(qc *config.Queue) *SqsQueue {
	config := &aws.Config{
		Region:   &qc.Region,
		LogLevel: aws.LogLevel(aws.LogOff),
		// LogLevel can be set to LogDebugWithHTTPBody for debugging purposes.
	}
	if s := session.Must(session.NewSession(config)); s != nil {
		return &SqsQueue{
			svc:    sqs.New(s, config),
			url:    qc.Url,
			wait:   qc.Wait,
			logger: logging.GetLogger(" < sqs > "),
		}
	} else {
		return nil
	}
}
