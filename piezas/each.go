package piezas

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/samwooo/bolsa/job"
	"github.com/samwooo/bolsa/job/feeder"
	"github.com/samwooo/bolsa/logging"
)

type eachJ struct {
	*job.Job
	iterator func(interface{}) (interface{}, error)
}

func (myself *eachJ) Work(p interface{}) (r interface{}, e error) {
	if myself.iterator != nil {
		return myself.iterator(p)
	} else {
		return p, nil
	}
}

func Each(ctx context.Context, logger logging.Logger, data []interface{},
	ite func(interface{}) (interface{}, error)) *sync.Map {
	start := time.Now()
	f := feeder.NewDataFeeder(ctx, logger, runtime.NumCPU(), data, 1, true)
	e := &eachJ{job.NewJob(logger, "Each", 0, f), ite}
	done := e.SetLaborStrategy(e).Run()
	e.Logger.Infof("done in %+v with %+v", time.Since(start), done)
	return done
}
