package piezas

import (
	"context"
	"time"

	"fmt"

	"strings"

	"github.com/samwooo/bolsa/gadgets/job"
	"github.com/samwooo/bolsa/gadgets/logging"
	"github.com/samwooo/bolsa/gadgets/util"
)

type reduceJ struct {
	*job.Job
	dataSize int
	memo     interface{}
	iterator func(v interface{}, memo interface{}) (interface{}, error)
}

func (myself *reduceJ) size() int {
	return myself.dataSize
}

func (*reduceJ) batch(ctx context.Context, groupedMash []interface{}) (interface{}, error) {
	return groupedMash, nil
}

func (myself *reduceJ) act(ctx context.Context, p interface{}) (r interface{}, e error) {
	var reasons []string
	if myself.iterator != nil {
		if data, ok := p.([]interface{}); !ok {
			reasons = append(reasons, fmt.Sprintf("cast %+v error", p))
		} else {
			m := myself.memo
			for _, d := range data {
				var err error
				if m, err = myself.iterator(d, m); err != nil {
					reasons = append(reasons, err.Error())
				}
			}
			myself.memo = m
		}
	}
	return myself.memo, util.ErrorFromString(strings.Join(reasons, " | "))
}

func Reduce(ctx context.Context, logger logging.Logger, data []interface{}, memo interface{},
	iterator func(interface{}, interface{}) (interface{}, error)) (interface{}, error) {

	start := time.Now()
	r := &reduceJ{job.NewJob(logger, 1), len(data), memo, iterator}
	done := r.BatchWanted(r).ActionWanted(r).Run(ctx, data)
	r.Logger.Infof("done in %+v with %+v", time.Since(start), done)
	if len(done) <= 0 {
		return memo, fmt.Errorf("unknown error")
	} else {
		return done[0].R, done[0].E
	}
}
