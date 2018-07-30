package piezas

import (
	"context"
	"time"

	"github.com/samwooo/bolsa/common/job"
	"github.com/samwooo/bolsa/common/job/feeder"
	"github.com/samwooo/bolsa/common/job/model"
	"github.com/samwooo/bolsa/common/logging"
)

type everyJ struct {
	*job.Job
	iterator func(interface{}) (bool, error)
}

func (myself *everyJ) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	if myself.iterator != nil {
		return myself.iterator(p)
	} else {
		return false, nil
	}
}

func Every(ctx context.Context, logger logging.Logger, data []interface{},
	iterator func(interface{}) (bool, error)) bool {

	start := time.Now()
	f := feeder.NewDataFeeder(ctx, logger, data, 1, true)
	e := &everyJ{job.NewJob(logger, "every", 0, f), iterator}
	r := e.LaborStrategy(e).Run(ctx)
	e.Logger.Infof("done in %+v with %+v", time.Since(start), r)
	pass := true
	r.Range(func(key, value interface{}) bool {
		if d, ok := value.(model.Done); ok {
			if d.E != nil {
				pass = false
				return false
			} else {
				if r, ok := d.R.(bool); !ok {
					pass = false
					return false
				} else {
					pass = pass && r
					return true
				}
			}
		} else {
			pass = false
			return false
		}
	})
	return pass
}
