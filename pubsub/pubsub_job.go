package pubsub

import (
	"context"
	"fmt"

	"github.com/samwooo/bolsa/job"
	"github.com/samwooo/bolsa/job/feeder"
	"github.com/samwooo/bolsa/job/model"
	"github.com/samwooo/bolsa/logging"
)

///////////////
// pub job //
type PubJob struct {
	*job.Job
	broker    Broker
	batchSize int
	maxRetry  int
}

func (pj *PubJob) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	if pj.batchSize != 1 {
		if pIsArray, ok := p.([]interface{}); ok {
			return pj.broker.Push(ctx, pIsArray)
		} else {
			return nil, fmt.Errorf("push failed: expect P to be an array")
		}
	} else {
		return pj.broker.Push(ctx, []interface{}{p})
	}
}
func (pj *PubJob) Worth(done model.Done) bool { return done.E != nil }
func (pj *PubJob) Limit() int                 { return pj.maxRetry }

func NewPubJob(ctx context.Context, logger logging.Logger, broker Broker, data []interface{},
	batchSize, maxRetry int, async bool) *PubJob {
	return &PubJob{job.NewJob(logger, "pubJob", 0, feeder.NewDataFeeder(ctx, logger,
		data, batchSize, !async)), broker, batchSize, maxRetry}
}

///////////////
// sub job //
type SubJob struct {
	*job.Job
	broker Broker
	handle MessageHandler
}

func (pj *SubJob) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	if pj.handle != nil {
		if messageBody, ok := p.([]interface{}); ok {
			if err := pj.handle(ctx, messageBody); err != nil {
				return nil, err
			} else {
				return nil, nil
			}
		} else {
			return nil, fmt.Errorf("poll filed: cast %+v to body error", p)
		}
	} else {
		return nil, fmt.Errorf("poll filed: missing handler")
	}
}

func NewSubJob(ctx context.Context, logger logging.Logger, broker Broker, handle MessageHandler) *SubJob {
	return &SubJob{job.NewJob(logger, "subJob", 0, feeder.NewChanFeeder(ctx, logger,
		func(p interface{}) (r interface{}, e error) {
			return broker.Poll(ctx)
		})), broker, handle}
}
