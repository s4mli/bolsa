package piezas

import (
	"context"
	"sync"
	"time"

	"github.com/samwooo/bolsa/common/job"
	"github.com/samwooo/bolsa/common/logging"
)

type eachJ struct {
	*job.Job
	iterator func(interface{}) (interface{}, error)
}

func (myself *eachJ) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	if myself.iterator != nil {
		return myself.iterator(p)
	} else {
		return p, nil
	}
}

func Each(ctx context.Context, logger logging.Logger, data []interface{},
	ite func(interface{}) (interface{}, error)) *sync.Map {
	start := time.Now()
	f := job.NewRetryableFeeder(ctx, data, 1, true)
	e := &eachJ{job.NewJob(logger, "Each", 0, f), ite}
	done := e.LaborStrategy(e).Run(ctx)
	e.Logger.Infof("done in %+v with %+v", time.Since(start), done)
	return done
}
