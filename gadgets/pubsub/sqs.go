package pubsub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/samwooo/bolsa/gadgets/job"
	"github.com/samwooo/bolsa/gadgets/logging"
	"github.com/samwooo/bolsa/gadgets/util"
)

///////////////////
// QueueMessage //
type SQSQueueMessage struct {
	queue         *SQSQueue
	messageId     string
	receiptHandle string
	body          string
}

func (m *SQSQueueMessage) Id() MessageId {
	return MessageId(m.messageId)
}

func (m *SQSQueueMessage) Ack() error {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(m.queue.url),
		ReceiptHandle: aws.String(m.receiptHandle),
	}
	if resp, err := m.queue.svc.DeleteMessage(params); err != nil {
		m.queue.logger.Errorf("Ack err: %s", err.Error())
		return err
	} else {
		m.queue.logger.Debugf("Ack done: %+v.", resp)
		return nil
	}
}

func (m *SQSQueueMessage) Parse() ([]interface{}, error) {
	if body, err := m.queue.decode(m.body); err != nil {
		m.queue.logger.Error(err)
		return nil, err
	} else {
		return body, nil
	}
}

////////////////////
// Queue Put Job //
type putJob struct {
	*job.Job
	q        *SQSQueue
	curRetry int
	maxRetry int
}

// batchStrategy
func (qt *putJob) size() int {
	return qt.q.batch
}

func (qt *putJob) batch(ctx context.Context, groupedMash []interface{}) (interface{}, error) {
	return qt.q.encode(groupedMash)
}

// actionStrategy
func (qt *putJob) act(ctx context.Context, p interface{}) (r interface{}, e error) {
	if bodyStr, ok := p.(string); ok {
		params := &sqs.SendMessageInput{
			MessageBody:  aws.String(bodyStr),
			QueueUrl:     aws.String(qt.q.url),
			DelaySeconds: aws.Int64(qt.q.delay),
		}
		if resp, err := qt.q.svc.SendMessageWithContext(ctx, params); err != nil {
			qt.q.logger.Error(err)
			return []MessageId{}, err
		} else {
			qt.q.logger.Debugf("put done: %+v", resp)
			return MessageId(aws.StringValue(resp.MessageId)), nil
		}
	} else {
		return nil, fmt.Errorf("cast body failed")
	}
}

// retryStrategy
func (qt *putJob) worth(done job.Done) bool {
	return done.E != nil
}

func (qt *putJob) forgo() bool {
	ended := qt.curRetry >= qt.maxRetry
	qt.curRetry++
	return ended
}

////////////
// Queue //
type SQSQueue struct {
	svc    *sqs.SQS
	url    string
	wait   int64
	delay  int64
	batch  int
	logger logging.Logger
}

func (q *SQSQueue) encode(body []interface{}) (string, error) {
	if payload, err := json.Marshal(body); err != nil {
		return "", err
	} else {
		return base64.StdEncoding.EncodeToString(payload), nil
	}
}

func (q *SQSQueue) decode(message string) ([]interface{}, error) {
	if payload, err := base64.StdEncoding.DecodeString(message); err != nil {
		return nil, err
	} else {
		var body []interface{}
		if e := json.Unmarshal(payload, body); e != nil {
			return nil, e
		} else {
			return body, nil
		}
	}
}

func (q *SQSQueue) Put(ctx context.Context, body []interface{}) ([]MessageId, error) {
	handleResults := func(allDone []job.Done) ([]MessageId, error) {
		var messageIds []MessageId
		errStr := "|"
		for _, done := range allDone {
			if done.E != nil {
				errStr += done.E.Error() + "|"
			}
			if done.R != nil {
				if messageId, ok := done.R.(MessageId); ok {
					messageIds = append(messageIds, messageId)
				} else {
					errStr += "cast messageId failed|"
				}
			}
		}
		return messageIds, util.ErrorFromString(errStr)
	}

	qt := putJob{job.NewJob(q.logger, 0), q, 0, 3}
	return handleResults(qt.BatchWanted(qt).ActionWanted(qt).RetryWanted(qt).Run(ctx, body))
}

func (q *SQSQueue) Pop(ctx context.Context) (Message, error) {
	stop := false
	go func() {
		<-ctx.Done()
		stop = true
	}()

	pollMessage := func() (*sqs.Message, error) {
		params := &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(q.url),
			MaxNumberOfMessages: aws.Int64(1),
			WaitTimeSeconds:     aws.Int64(q.wait),
		}
		for !stop {
			if resp, err := q.svc.ReceiveMessageWithContext(ctx, params); err != nil {
				return nil, err
			} else {
				if len(resp.Messages) == 1 {
					q.logger.Debugf("pop done: %+v", resp)
					return resp.Messages[0], nil
				}
				if len(resp.Messages) > 1 {
					return nil, fmt.Errorf("got %d( > 1 ) messages", len(resp.Messages))
				}
			}
		}
		return nil, fmt.Errorf("reached unreachable statement")
	}

	if awsMsg, err := pollMessage(); err != nil {
		q.logger.Error(err)
		return nil, err
	} else {
		return &SQSQueueMessage{
			queue:         q,
			receiptHandle: *awsMsg.ReceiptHandle,
			messageId:     *awsMsg.MessageId,
			body:          *awsMsg.Body,
		}, nil
	}
}

func (q *SQSQueue) Len() (uint64, error) {
	if aws.BoolValue(q.svc.Config.DisableSSL) {
		// This is a local dev mode server, which for now does not support length
		return 0, nil
	}
	res, err := q.svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{aws.String("ApproximateNumberOfMessages")},
		QueueUrl:       aws.String(q.url),
	})
	if err != nil {
		q.logger.Error(err)
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

// Create a new SQS queue instance bound to the specified url. waitTime is the number of seconds
// that an sqs.ReceiveMessage() should wait, at most, for a message to arrive. If it is set to a
// non-zero number then long-polling is enabled, as described here:
// http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
func NewSQSQueue(region, url string, wait, delay int64, batch int) *SQSQueue {
	config := &aws.Config{
		Region:   &region,
		LogLevel: aws.LogLevel(aws.LogOff), // LogLevel can be set to LogDebugWithHTTPBody for debugging purposes.
	}
	if s := session.Must(session.NewSession(config)); s != nil {
		return &SQSQueue{
			svc:    sqs.New(s, config),
			url:    url,
			wait:   wait,
			delay:  delay,
			batch:  batch,
			logger: logging.GetLogger(" < sqs > "),
		}
	} else {
		return nil
	}
}
