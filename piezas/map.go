package piezas

import (
	"context"
	"runtime"
	"time"

	"github.com/samwooo/bolsa/job"
	"github.com/samwooo/bolsa/job/feeder"
	"github.com/samwooo/bolsa/job/model"
)

type mapJ struct {
	*job.Job
	iterator func(interface{}) (interface{}, error)
}

func (myself *mapJ) Work(p interface{}) (r interface{}, e error) {
	if myself.iterator != nil {
		return myself.iterator(p)
	} else {
		return p, nil
	}
}

func Map(ctx context.Context, data []interface{}, iterator func(interface{}) (interface{}, error)) []interface{} {

	start := time.Now()
	f := feeder.NewDataFeeder(ctx, "MapFeeder", runtime.NumCPU(), data, 1, true)
	e := &mapJ{job.NewJob("Map", 0, f), iterator}
	r := e.SetLaborStrategy(e).Run()
	e.Logger.Infof("done in %+v with %+v", time.Since(start), r)
	var result []interface{}
	r.Range(func(key, value interface{}) bool {
		if d, ok := value.(model.Done); ok {
			if d.E == nil && d.R != nil {
				result = append(result, d.R)
			}
		}
		return true
	})
	return result
}
