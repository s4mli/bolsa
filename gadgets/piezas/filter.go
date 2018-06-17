package piezas

import (
	"context"
	"time"

	"github.com/samwooo/bolsa/gadgets/job"
	"github.com/samwooo/bolsa/gadgets/logging"
)

type filterJ struct {
	*job.Job
	iterator func(interface{}) (bool, error)
}

func (myself *filterJ) doAction(ctx context.Context, p interface{}) (r interface{}, e error) {
	if myself.iterator != nil {
		return myself.iterator(p)
	} else {
		return true, nil
	}
}

func Filter(ctx context.Context, logger logging.Logger, data []interface{},
	iterator func(interface{}) (bool, error)) []interface{} {

	start := time.Now()
	f := &filterJ{job.NewJob(logger, 0), iterator}
	done := f.ActionWanted(f).Run(ctx, data)
	f.Logger.Infof("done in %+v with %+v", time.Since(start), done)
	var result []interface{}
	for _, d := range done {
		if d.E == nil {
			if v, ok := d.R.(bool); ok && v == true {
				result = append(result, d.P)
			}
		}
	}
	return result
}
