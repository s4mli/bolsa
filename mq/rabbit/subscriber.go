package rabbit

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/samwooo/bolsa/job"
	"github.com/samwooo/bolsa/job/feeder"
	"github.com/samwooo/bolsa/job/model"
	"github.com/samwooo/bolsa/logging"
	"github.com/streadway/amqp"
)

type Subscriber struct {
	qConn atomic.Value
	qChan atomic.Value
	qName string
	*job.Job
}

func (s *Subscriber) exit(chan model.Done) error {
	for {
		if conn, ok := s.qConn.Load().(*amqp.Connection); ok && conn != nil {
			conn.Close()
			s.Logger.Infof("subscriber %s closed", s.qName)
			break
		} else {
			s.Logger.Warn("close cast failed")
			time.Sleep(time.Millisecond * 50)
		}
	}
	return nil
}

// in order to fit the JobFeeder pattern, here we are using Get fn.
// but according to RabbitMQ, we really should use Consume instead.
func (s *Subscriber) consume(labor model.Labor) error {
	if qChan, ok := s.qChan.Load().(*amqp.Channel); ok && qChan != nil {
		return retrieve(s.Logger, qChan, s.qName, labor)
	} else {
		msg := "consume cast failed"
		s.Logger.Warn(msg)
		return fmt.Errorf(msg)
	}
}

func (s *Subscriber) connect(qUser, qPassword, qUri string) {
	qConn, qChan := connect(s.Logger, qUser, qPassword, qUri, s.connect)
	s.qConn.Store(qConn)
	s.qChan.Store(qChan)
}

func NewSubscriber(ctx context.Context, logger logging.Logger, qName, qUser, qPassword, qUri string, qWorkers int,
	labor model.Labor) *Subscriber {
	s := &Subscriber{qName: qName}
	s.Job = job.NewJob(logger, fmt.Sprintf("| %s subscriber | ", qName), qWorkers,
		feeder.NewWorkFeeder(ctx, logger, qWorkers, nil, s.consume, labor, s.exit))
	s.connect(qUser, qPassword, qUri)
	return s
}
