package piezas

import (
	"context"
	"time"

	"github.com/samwooo/bolsa/gadgets/job"
	"github.com/samwooo/bolsa/gadgets/logging"
)

type everyJ struct {
	*job.Job
	iterator func(interface{}) (bool, error)
}

func (myself *everyJ) act(ctx context.Context, p interface{}) (r interface{}, e error) {
	if myself.iterator != nil {
		return myself.iterator(p)
	} else {
		return false, nil
	}
}

func Every(ctx context.Context, logger logging.Logger, data []interface{},
	iterator func(interface{}) (bool, error)) bool {

	start := time.Now()
	e := &everyJ{job.NewJob(logger, 0), iterator}
	done := e.ActionWanted(e).Run(ctx, data)
	e.Logger.Infof("done in %+v with %+v", time.Since(start), done)
	for _, d := range done {
		if d.E != nil {
			return false
		} else {
			if r, ok := d.R.(bool); !ok || !r {
				return false
			}
		}
	}
	return true
}
