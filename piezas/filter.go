package piezas

import (
	"context"
	"runtime"
	"time"

	"github.com/samwooo/bolsa/job"
	"github.com/samwooo/bolsa/job/feeder"
	"github.com/samwooo/bolsa/job/model"
)

type filterJ struct {
	*job.Job
	iterator func(interface{}) (bool, error)
}

func (myself *filterJ) Work(p interface{}) (r interface{}, e error) {
	if myself.iterator != nil {
		return myself.iterator(p)
	} else {
		return true, nil
	}
}

func Filter(ctx context.Context, data []interface{}, iterator func(interface{}) (bool, error)) []interface{} {
	start := time.Now()
	f := feeder.NewDataFeeder(ctx, "FilterFeeder", runtime.NumCPU(), data, 1, true)
	filter := &filterJ{job.NewJob("Filter", 0, f), iterator}
	r := filter.SetLaborStrategy(filter).Run()
	filter.Logger.Infof("done in %+v with %+v", time.Since(start), r)
	var result []interface{}
	r.Range(func(key, value interface{}) bool {
		if d, ok := value.(model.Done); ok {
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
