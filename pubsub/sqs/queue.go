package sqs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/samwooo/bolsa/logging"
	"github.com/samwooo/bolsa/pubsub"
)

///////////////////////////
// SQSQueue is a Broker //
type Queue struct {
	svc    *sqs.SQS
	url    string
	wait   int64
	delay  int64
	Logger logging.Logger
}

func (q *Queue) encode(body []interface{}) (string, error) {
	if payload, err := json.Marshal(body); err != nil {
		return "", err
	} else {
		return base64.StdEncoding.EncodeToString(payload), nil
	}
}

func (q *Queue) decode(message string) ([]interface{}, error) {
	if payload, err := base64.StdEncoding.DecodeString(message); err != nil {
		return nil, err
	} else {
		var body interface{}
		if e := json.Unmarshal(payload, body); e != nil {
			return nil, e
		} else {
			if b, ok := body.([]interface{}); !ok {
				return nil, fmt.Errorf("decode failed: cast %+v to array error", body)
			} else {
				return b, nil
			}
		}
	}
}

func (q *Queue) Push(ctx context.Context, body []interface{}) (pubsub.MessageId, error) {
	sendMessage := func(payload string) (pubsub.MessageId, error) {
		params := &sqs.SendMessageInput{
			MessageBody:  aws.String(payload),
			QueueUrl:     aws.String(q.url),
			DelaySeconds: aws.Int64(q.delay),
		}
		if resp, err := q.svc.SendMessageWithContext(ctx, params); err != nil {
			q.Logger.Errorf("push failed: %s", err.Error())
			return pubsub.MessageId(aws.StringValue(nil)), err
		} else {
			q.Logger.Debugf("push succeed: %+v", resp)
			return pubsub.MessageId(aws.StringValue(resp.MessageId)), nil
		}
	}
	if payload, err := q.encode(body); err != nil {
		return pubsub.MessageId(aws.StringValue(nil)), err
	} else {
		return sendMessage(payload)
	}
}

func (q *Queue) Poll(ctx context.Context) ([]interface{}, error) {
	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.url),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(q.wait),
	}
	if resp, err := q.svc.ReceiveMessageWithContext(ctx, params); err != nil {
		return nil, err
	} else {
		if len(resp.Messages) == 1 {
			q.Logger.Debugf("poll succeed : %+v", resp)
			sqsMessage := resp.Messages[0]
			if body, err := q.decode(*sqsMessage.Body); err != nil {
				q.Logger.Errorf("poll failed: %s", err.Error())
				return nil, err
			} else {
				// delete Message from q
				defer func() {
					params := &sqs.DeleteMessageInput{
						QueueUrl:      aws.String(q.url),
						ReceiptHandle: sqsMessage.ReceiptHandle,
					}
					if resp, err := q.svc.DeleteMessage(params); err != nil {
						q.Logger.Errorf("delete failed: %s", err.Error())
					} else {
						q.Logger.Debugf("delete succeed: %+v.", resp)
					}
				}()
				return body, nil
			}
		} else {
			return nil, fmt.Errorf("polled %d messages", len(resp.Messages))
		}
	}
}

func (q *Queue) Len() (uint64, error) {
	if aws.BoolValue(q.svc.Config.DisableSSL) {
		return 0, nil
	}
	res, err := q.svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{aws.String("ApproximateNumberOfMessages")},
		QueueUrl:       aws.String(q.url),
	})
	if err != nil {
		q.Logger.Error(err)
		return 0, err
	}
	lenString := aws.StringValue(res.Attributes["ApproximateNumberOfMessages"])
	return strconv.ParseUint(lenString, 10, 64)
}

/*
func (q *Queue) putBatch(ctx context.Context, body []interface{}) ([]MessageId, error) {
	errorFromString := func( s string ) error {
		if s == "" {
			return nil
		} else {
			return fmt.Errorf(s)
		}
	}

	sendBatch := func(p interface{}) (r interface{}, e error) {
		if params, ok := p.(*sqs.SendMessageBatchInput); !ok {
			errStr := "cast sendMessageBatchInput failed"
			q.logger.Error(errStr)
			return nil, fmt.Errorf(errStr)
		} else {
			if resp, err := q.svc.SendMessageBatchWithContext(ctx, params); err != nil {
				return nil, err
			} else {
				var messageIds []MessageId{}
				if len(resp.Successful) > 0 {
					for _, s := range resp.Successful {
						messageIds = append(messageIds, MessageId(*s.MessageId))
					}
				}
				errStr := ""
				if len(resp.Failed) > 0 {
					for _, f := range resp.Failed {
						errStr += f.String()
					}
				}
				return messageIds, errorFromString(errStr)
			}
		}
	}

	bodyLen := len(body)
	var batchParams []interface{}
	for i := 0; i < 1+bodyLen/(10*q.batch); i++ {
		bodyStart := i * q.batch * 10
		bodyEnd := bodyStart + q.batch*10
		if bodyEnd > bodyLen {
			bodyEnd = bodyLen
		}
		batchBody := body[bodyStart:bodyEnd]
		batchBodyLen := len(batchBody)
		var entries []*sqs.SendMessageBatchRequestEntry
		for k := 0; k < 1+batchBodyLen/q.batch; k++ {
			batchBodyStart := k * q.batch
			batchBodyEnd := batchBodyStart + q.batch
			if batchBodyEnd > batchBodyLen {
				batchBodyEnd = batchBodyLen
			}
			if bodyStr, err := q.encode(batchBody[batchBodyStart:batchBodyEnd]); err != nil {
				q.logger.Error(err)
				continue
			} else {
				entries = append(entries, &sqs.SendMessageBatchRequestEntry{
					Id:           aws.String(fmt.Sprintf("%d", k)),
					MessageBody:  aws.String(bodyStr),
					DelaySeconds: aws.Int64(q.delay),
				})
			}
		}
		batchParams = append(batchParams, &sqs.SendMessageBatchInput{
			Entries:  entries,
			QueueUrl: aws.String(q.url),
		})
	}
	if len(batchParams) > 0 {
		allDone := RunTask(batchParams, sendBatch, len(batchParams), logging.GetLogger("SQS"))
		var messageIds []MessageId
		errStr := ""
		for _, done := range allDone {
			if ids, ok := done.R.([]MessageId); ok {
				messageIds = append(messageIds, ids...)
			}
			if done.E != nil {
				errStr += done.E.Error()
			}
		}
		return messageIds, errorFromString(errStr)
	}
	return nil, nil
}
*/

// Create a new SQS Queue instance bound to the specified url. waitTime is the number of seconds
// that an sqs.ReceiveMessage() should wait, at most, for a message to arrive. If it is set to a
// non-zero number then long-polling is enabled, as described here:
// http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
//func NewSQS(region, url string, wait, delay int64, batch int) *SQSQueue {
func NewQueue(region, url string, wait, delay int64) *Queue {
	config := &aws.Config{
		Region:   &region,
		LogLevel: aws.LogLevel(aws.LogOff), // LogLevel can be set to LogDebugWithHTTPBody for debugging purposes.
	}
	if s := session.Must(session.NewSession(config)); s != nil {
		return &Queue{
			svc:    sqs.New(s, config),
			url:    url,
			wait:   wait,
			delay:  delay,
			Logger: logging.GetLogger("sqs "),
		}
	} else {
		return nil
	}
}
