package piezas

import (
	"context"
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
	ite func(interface{}) (interface{}, error)) []job.Done {

	start := time.Now()
	e := &eachJ{job.NewJob(logger, "Each", 0), ite}
	done := e.LaborStrategy(e).Run(ctx, job.NewDataFeeder(data))
	e.Logger.Infof("done in %+v with %+v", time.Since(start), done)
	return done
}
