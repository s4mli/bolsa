package piezas

import (
	"context"

	"time"

	"github.com/samwooo/bolsa/gadgets/job"
	"github.com/samwooo/bolsa/gadgets/logging"
)

type eachJ struct {
	*job.Job
	iterator func(interface{}) (interface{}, error)
}

func (myself *eachJ) doAction(ctx context.Context, p interface{}) (r interface{}, e error) {
	if myself.iterator != nil {
		return myself.iterator(p)
	} else {
		return p, nil
	}
}

func Each(ctx context.Context, logger logging.Logger, data []interface{},
	iterator func(interface{}) (interface{}, error)) []job.Done {

	start := time.Now()
	e := &eachJ{job.NewJob(logger, 0), iterator}
	done := e.ActionWanted(e).Run(ctx, data)
	e.Logger.Infof("done in %+v with %+v", time.Since(start), done)
	return done
}
