package piezas

import (
	"context"
	"time"

	"github.com/samwooo/bolsa/common/job"
	"github.com/samwooo/bolsa/common/logging"
)

type mapJ struct {
	*job.Job
	iterator func(interface{}) (interface{}, error)
}

func (myself *mapJ) act(ctx context.Context, p interface{}) (r interface{}, e error) {
	if myself.iterator != nil {
		return myself.iterator(p)
	} else {
		return p, nil
	}
}

func Map(ctx context.Context, logger logging.Logger, data []interface{},
	iterator func(interface{}) (interface{}, error)) []interface{} {

	start := time.Now()
	e := &mapJ{job.NewJob(logger, 0), iterator}
	done := e.ActionHandler(e).Run(ctx, data)
	e.Logger.Infof("done in %+v with %+v", time.Since(start), done)
	var result []interface{}
	for _, d := range done {
		if d.E == nil && d.R != nil {
			result = append(result, d.R)
		}
	}
	return result
}
