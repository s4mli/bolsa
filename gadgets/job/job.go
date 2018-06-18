package job

import (
	"context"

	"runtime"

	"fmt"

	"github.com/samwooo/bolsa/gadgets/logging"
)

type Job struct {
	Logger  logging.Logger
	workers int
	batchStrategy
	actionStrategy
	retryStrategy
}

func (j *Job) feed(ctx context.Context, mash []interface{}) <-chan interface{} {
	type batch func(context.Context, []interface{}) (interface{}, error)

	feedWithBatch := func(batchSize int, batch batch) <-chan interface{} {
		mashLen := len(mash)
		group := mashLen / batchSize
		if mashLen%batchSize > 0 {
			group += 1
		}
		in := make(chan interface{}, group)
		waiter := make(chan bool, group)
		for k := 0; k < group; k++ {
			start := k * batchSize
			end := start + batchSize
			if end > mashLen {
				end = mashLen
			}
			go func(groupedMash []interface{}, in chan<- interface{}, waiter chan<- bool) {
				if data, err := batch(ctx, groupedMash); err != nil {
					// TODO: how to handle batch error gracefully
					j.Logger.Errorf("batch( %d ) failed( %s ) for %+v", batchSize, err.Error(), groupedMash)
				} else {
					j.Logger.Debugf("batch( %d ) done( %+v ) for %+v", batchSize, data, groupedMash)
					in <- data
				}
				waiter <- true
			}(mash[start:end], in, waiter)
		}
		go func(in chan<- interface{}, waiter <-chan bool) {
			for i := 0; i < group; i++ {
				<-waiter
			}
			close(in)
		}(in, waiter)
		return in
	}
	if j.batchStrategy != nil {
		j.Logger.Debugf(" * batch needed size %d", j.batchStrategy.size())
		return feedWithBatch(j.batchStrategy.size(), j.batchStrategy.batch)
	} else {
		j.Logger.Debug(" * batch not needed")
		return feedWithBatch(1, func(ctx context.Context, mash []interface{}) (interface{}, error) {
			return mash[0], nil
		})
	}
}

func (j *Job) chew(ctx context.Context, in <-chan interface{}) <-chan Done {
	type act func(ctx context.Context, p interface{}) (r interface{}, e error)

	chewWithAction := func(action act) <-chan Done {
		out := make(chan Done, j.workers)
		waiter := make(chan bool, j.workers)
		for i := 0; i < j.workers; i++ {
			go func(in <-chan interface{}, out chan<- Done, waiter chan<- bool) {
				for para := range in {
					ret, err := action(ctx, para)
					if err != nil {
						j.Logger.Errorf("action failed( %s ) for %+v", err.Error(), para)
					} else {
						j.Logger.Debugf("action done( %+v ) for %+v", ret, para)
					}
					out <- Done{para, ret, err}
				}
				waiter <- true
			}(in, out, waiter)
		}
		go func(out chan<- Done, waiter <-chan bool) {
			for i := 0; i < j.workers; i++ {
				<-waiter
			}
			close(out)
		}(out, waiter)
		return out
	}
	if j.actionStrategy != nil {
		j.Logger.Debugf(" * action needed workers %d", j.workers)
		return chewWithAction(j.actionStrategy.act)
	} else {
		j.Logger.Debug(" * action not needed")
		return chewWithAction(func(ctx context.Context, para interface{}) (interface{}, error) {
			return para, nil
		})
	}
}

func (j *Job) digest(ctx context.Context, out <-chan Done) <-chan []Done {
	output := make(chan []Done)
	go func(out <-chan Done, output chan<- []Done) {
		var results []Done
		for r := range out {
			results = append(results, r)
		}
		j.Logger.Debugf(" *** done %d", len(results))
		output <- results
	}(out, output)
	return output
}

func (j *Job) run(ctx context.Context, with []interface{}) []Done {
	return <-j.digest(ctx, j.chew(ctx, j.feed(ctx, with)))
}

func (j *Job) BatchWanted(bs batchStrategy) *Job {
	j.batchStrategy = bs
	return j
}

func (j *Job) ActionWanted(as actionStrategy) *Job {
	j.actionStrategy = as
	return j
}

func (j *Job) RetryWanted(rs retryStrategy) *Job {
	j.retryStrategy = rs
	return j
}

func (j *Job) Run(ctx context.Context, with []interface{}) []Done {
	child, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	var finalAllDone []Done
	allDone := j.run(child, with)
	if j.retryStrategy != nil {
		for {
			var retries []interface{}
			for _, done := range allDone {
				if j.retryStrategy.worth(done) {
					retries = append(retries, done.P)
				} else {
					finalAllDone = append(finalAllDone, done)
				}
			}
			if len(retries) <= 0 || j.retryStrategy.forgo() {
				for _, r := range retries {
					finalAllDone = append(finalAllDone, Done{r, nil, fmt.Errorf("need more retry")})
				}
				j.Logger.Debug(" * retry ended")
				break
			} else {
				j.Logger.Debugf(" * retry started: %d", len(retries))
				workers := j.workers
				if j.batchStrategy != nil {
					workers /= j.batchStrategy.size()
				}
				allDone = NewJob(j.Logger, workers).ActionWanted(j.actionStrategy).run(child, retries)
			}
		}
	} else {
		finalAllDone = allDone
	}
	j.Logger.Debugf(" *** final done %d ( %+v )", len(finalAllDone), finalAllDone)
	return finalAllDone
}

func NewJob(logger logging.Logger, workers int) *Job {
	if workers <= 0 {
		workers = runtime.NumCPU() * 64
	}
	return &Job{logger, workers, nil, nil, nil}
}
