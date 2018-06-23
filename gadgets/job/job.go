package job

import (
	"context"

	"runtime"

	"fmt"

	"math"

	"github.com/samwooo/bolsa/gadgets/logging"
)

type strategyType int

const (
	Batch strategyType = iota
	Action
	Retry
)

func (st *strategyType) String() string {
	switch *st {
	case Batch:
		return "batch"
	case Action:
		return "action"
	case Retry:
		return "retry"
	}
	return "?"
}

////////////
// Error //
type Error struct {
	Strategy strategyType
	error
}

func (je Error) Error() string {
	return fmt.Sprintf("× %s failed: %s", je.Strategy.String(), je.error.Error())
}

func newError(st strategyType, err error) *Error {
	return &Error{st, err}
}

//////////
// Job //
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
					j.Logger.Errorf("× batch failed ( %+v ) %s", groupedMash, err.Error())
					in <- Done{groupedMash, data, newError(Batch, err)}
				} else {
					j.Logger.Debugf("√ batch done ( %+v ) %+v", groupedMash, data)
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
		j.Logger.Debugf("batch √ size %d", j.batchStrategy.size())
		return feedWithBatch(j.batchStrategy.size(), j.batchStrategy.batch)
	} else {
		j.Logger.Debug("batch ×")
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
					if done, ok := para.(Done); ok {
						j.Logger.Debugf("√ action done, pipe batch error ( %s ) through", done.E.Error())
						out <- done // batch error
					} else {
						ret, err := action(ctx, para)
						if err != nil {
							j.Logger.Errorf("× action failed ( %+v ) %s ", para, err.Error())
							out <- Done{para, ret, newError(Action, err)}
						} else {
							j.Logger.Debugf("√ action done ( %+v ) %+v", para, ret)
							out <- Done{para, ret, nil}
						}
					}
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
		j.Logger.Debugf("action √ workers %d", j.workers)
		return chewWithAction(j.actionStrategy.act)
	} else {
		j.Logger.Debug("action ×")
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
		output <- results
	}(out, output)
	return output
}

func (j *Job) run(ctx context.Context, with []interface{}) []Done {
	return <-j.digest(ctx, j.chew(ctx, j.feed(ctx, with)))
}

////////////////
// Set Batch //
func (j *Job) BatchWanted(bs batchStrategy) *Job {
	j.batchStrategy = bs
	return j
}

/////////////////
// Set Action //
func (j *Job) ActionWanted(as actionStrategy) *Job {
	j.actionStrategy = as
	return j
}

////////////////
// Set Retry //
func (j *Job) RetryWanted(rs retryStrategy) *Job {
	j.retryStrategy = rs
	return j
}

func (j *Job) Run(ctx context.Context, with []interface{}) []Done {
	j.workers = int(math.Min(float64(j.workers), float64(len(with))))
	child, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	var finalAllDone []Done
	allDone := j.run(child, with)
	if j.retryStrategy == nil {
		j.Logger.Debugf("retry ×")
		finalAllDone = allDone
	} else {
		for {
			var actionRetries []interface{}
			var batchRetries []interface{}
			for _, done := range allDone {
				if done.E != nil && j.retryStrategy.worth(done) { // no error then skip retry even you say its worthy
					if e, ok := done.E.(*Error); ok && e.Strategy == Batch {
						if groupedPara, isArray := done.P.([]interface{}); isArray {
							batchRetries = append(batchRetries, groupedPara...)
						} else {
							j.Logger.Error("× cast para failed, skip retry")
						}
					} else {
						actionRetries = append(actionRetries, done.P)
					}
				} else {
					finalAllDone = append(finalAllDone, done)
				}
			}

			if j.retryStrategy.forgo() || (len(actionRetries) <= 0 && len(batchRetries) <= 0) {
				for _, ar := range actionRetries {
					finalAllDone = append(finalAllDone, Done{ar, nil, fmt.Errorf("action ×")})
				}
				for _, br := range batchRetries {
					finalAllDone = append(finalAllDone, Done{br, nil, fmt.Errorf("batch ×")})
				}
				j.Logger.Debug("√ retry ended")
				break
			} else {
				j.Logger.Debugf("√ retry started ( batch %d, action %d )", len(batchRetries), len(actionRetries))
				allDone = []Done{}
				if len(actionRetries) > 0 {
					allDone = append(allDone, NewJob(j.Logger, j.workers).ActionWanted(
						j.actionStrategy).run(child, actionRetries)...)
				}
				if len(batchRetries) > 0 {
					allDone = append(allDone, NewJob(j.Logger, j.workers).BatchWanted(j.batchStrategy).ActionWanted(
						j.actionStrategy).run(child, batchRetries)...)
				}
			}
		}
	}
	j.Logger.Debugf("√ finished ( %+v )", finalAllDone)
	return finalAllDone
}

func NewJob(logger logging.Logger, workers int) *Job {
	if workers <= 0 {
		workers = runtime.NumCPU() * 64
	}
	return &Job{logger, workers, nil, nil, nil}
}
