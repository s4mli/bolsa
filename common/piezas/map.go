package piezas

import (
	"context"
	"time"

	"github.com/samwooo/bolsa/common/job"
	"github.com/samwooo/bolsa/common/job/feeder"
	"github.com/samwooo/bolsa/common/job/share"
	"github.com/samwooo/bolsa/common/logging"
)

type mapJ struct {
	*job.Job
	iterator func(interface{}) (interface{}, error)
}

func (myself *mapJ) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	if myself.iterator != nil {
		return myself.iterator(p)
	} else {
		return p, nil
	}
}

func Map(ctx context.Context, logger logging.Logger, data []interface{},
	iterator func(interface{}) (interface{}, error)) []interface{} {

	start := time.Now()
	f := feeder.NewDataFeeder(ctx, logger, data, 1, true)
	e := &mapJ{job.NewJob(logger, "Map", 0, f), iterator}
	r := e.LaborStrategy(e).Run(ctx)
	e.Logger.Infof("done in %+v with %+v", time.Since(start), r)
	var result []interface{}
	r.Range(func(key, value interface{}) bool {
		if d, ok := value.(share.Done); ok {
			if d.E == nil && d.R != nil {
				result = append(result, d.R)
			}
		}
		return true
	})
	return result
}
