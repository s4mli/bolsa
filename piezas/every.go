package piezas

import (
	"context"
	"runtime"
	"time"

	"github.com/samwooo/bolsa/job"
	"github.com/samwooo/bolsa/job/feeder"
	"github.com/samwooo/bolsa/job/model"
)

type everyJ struct {
	*job.Job
	iterator func(interface{}) (bool, error)
}

func (myself *everyJ) Work(p interface{}) (r interface{}, e error) {
	if myself.iterator != nil {
		return myself.iterator(p)
	} else {
		return false, nil
	}
}

func Every(ctx context.Context, data []interface{}, iterator func(interface{}) (bool, error)) bool {
	start := time.Now()
	f := feeder.NewDataFeeder(ctx, "EveryFeeder", runtime.NumCPU(), data, 1, true)
	e := &everyJ{job.NewJob("Every", 0, f), iterator}
	r := e.SetLaborStrategy(e).Run()
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
