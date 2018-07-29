package piezas

import (
	"context"
	"time"

	"github.com/samwooo/bolsa/common/job"
	"github.com/samwooo/bolsa/common/logging"
)

type filterJ struct {
	*job.Job
	iterator func(interface{}) (bool, error)
}

func (myself *filterJ) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	if myself.iterator != nil {
		return myself.iterator(p)
	} else {
		return true, nil
	}
}

func Filter(ctx context.Context, logger logging.Logger, data []interface{},
	iterator func(interface{}) (bool, error)) []interface{} {

	start := time.Now()
	f := job.NewDataFeeder(ctx, logger, data, 1, true)
	filter := &filterJ{job.NewJob(logger, "Filter", 0, f), iterator}
	r := filter.LaborStrategy(filter).Run(ctx)
	filter.Logger.Infof("done in %+v with %+v", time.Since(start), r)
	var result []interface{}
	r.Range(func(key, value interface{}) bool {
		if d, ok := value.(job.Done); ok {
			if d.E == nil {
				if v, ok := d.R.(bool); ok && v {
					result = append(result, d.D)
				}
			}
		}
		return true
	})
	return result
}
