package sqs

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/samwooo/bolsa/job"
	"github.com/samwooo/bolsa/job/feeder"
	"github.com/samwooo/bolsa/job/model"
	"github.com/samwooo/bolsa/logging"
)

type Subscriber struct {
	ctx      context.Context
	qService *sqs.SQS
	qUrl     string
	qWait    int64
	ready    atomic.Value
	*job.Job
}

func (s *Subscriber) exit(chan model.Done) error {
	s.ready.Store(false)
	return nil
}

func (s *Subscriber) consume(labor model.Labor) error {
	if ready, ok := s.ready.Load().(bool); ok && ready {
		return retrieve(s.ctx, s.Logger, s.qService, s.qUrl, s.qWait, labor)
	} else {
		return nil
	}
}

func (s *Subscriber) connect(qRegion string) {
	s.ready.Store(false)
	connect(s.Logger, qRegion)
	s.ready.Store(true)
}

func NewSubscriber(ctx context.Context, logger logging.Logger, qRegion, qUrl string, qWait int64, qWorkers int,
	labor model.Labor) *Subscriber {
	s := &Subscriber{ctx: ctx, qUrl: qUrl, qWait: qWait, ready: atomic.Value{}}
	s.Job = job.NewJob(logger, fmt.Sprintf("| ...%s subscriber | ", qUrl[int(math.Max(float64(len(qUrl)-9),
		float64(0))):]), qWorkers,
		feeder.NewWorkFeeder(ctx, logger, qWorkers, nil, s.consume, labor, s.exit))
	s.connect(qRegion)
	return s
}
