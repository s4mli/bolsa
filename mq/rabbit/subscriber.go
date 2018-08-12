package rebbit

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/samwooo/bolsa/job"
	"github.com/samwooo/bolsa/job/feeder"
	"github.com/samwooo/bolsa/job/model"
	"github.com/samwooo/bolsa/logging"
	"github.com/streadway/amqp"
)

type Subscriber struct {
	qConn *amqp.Connection
	qChan *amqp.Channel
	qName string
	ready atomic.Value
	*job.Job
}

func (s *Subscriber) exit(chan model.Done) error {
	s.qConn.Close()
	s.ready.Store(false)
	return nil
}

// in order to fit the JobFeeder pattern, here we are using Get fn.
// but according to RabbitMQ, we really should use Consume instead.
func (s *Subscriber) consume(labor model.Labor) error {
	if ready, ok := s.ready.Load().(bool); ok && ready {
		return retrieve(s.qChan, s.qName, labor)
	} else {
		return nil
	}
}

func (s *Subscriber) connect(user, password, uri string) {
	s.ready.Store(false)
	s.qConn, s.qChan = connect(s.Logger, user, password, uri, s.connect)
	s.ready.Store(true)
}

func NewSubscriber(ctx context.Context, logger logging.Logger, qName, qUser, qPassword, qUri string, qWorkers int,
	labor model.Labor) *Subscriber {
	s := &Subscriber{qName: qName, ready: atomic.Value{}}
	s.Job = job.NewJob(logger, fmt.Sprintf("| %s subscriber | ", qName), qWorkers,
		feeder.NewWorkFeeder(ctx, logger, qWorkers, nil, s.consume, labor, s.exit))
	s.connect(qUser, qPassword, qUri)
	return s
}
