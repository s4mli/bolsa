package piezas

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/common/job"
	"github.com/samwooo/bolsa/common/logging"
)

type reduceJ struct {
	*job.Job
	memo     interface{}
	iterator func(v interface{}, memo interface{}) (interface{}, error)
}

func (myself *reduceJ) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	var reasons []string
	if myself.iterator != nil {
		if data, ok := p.([]interface{}); !ok {
			reasons = append(reasons, fmt.Sprintf("cast %+v error", p))
		} else {
			var m = myself.memo
			var err error = nil
			for _, d := range data {
				if m, err = myself.iterator(d, m); err != nil {
					reasons = append(reasons, err.Error())
				}
			}
			myself.memo = m
		}
	}
	return myself.memo, common.ErrorFromString(strings.Join(reasons, " | "))
}

func Reduce(ctx context.Context, logger logging.Logger, data []interface{}, memo interface{},
	iterator func(interface{}, interface{}) (interface{}, error)) (m interface{}, e error) {

	start := time.Now()
	f := job.NewRetryableFeeder(ctx, data, 0, true)
	reduce := &reduceJ{job.NewJob(logger, "Reduce", 1, f), memo, iterator}
	r := reduce.LaborStrategy(reduce).Run(ctx)
	reduce.Logger.Infof("done in %+v with %+v", time.Since(start), r)
	r.Range(func(key, value interface{}) bool {
		if d, ok := value.(job.Done); ok {
			m, e = d.R, d.E
			return false
		} else {
			m, e = nil, nil
			return true
		}
	})
	return
}
