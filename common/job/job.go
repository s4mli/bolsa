package job

import (
	"context"
	"fmt"
	"runtime"

	"github.com/samwooo/bolsa/common/logging"
)

func (j *Job) feed(ctx context.Context, mash []interface{}) <-chan interface{} {
	type batchFn func(context.Context, []interface{}) (interface{}, error)

	feedWithBatch := func(batchSize int, batch batchFn) <-chan interface{} {
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
					j.Logger.Errorf("× batch failed ( %+v, %s )", groupedMash, err.Error())
					in <- Done{
						groupedMash,
						data,
						newError(Batch, fmt.Errorf("( %+v, %s )", groupedMash, err.Error()))}
				} else {
					j.Logger.Infof("√ batch succeed ( %+v, %+v )", groupedMash, data)
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
	if j.batchHandler != nil {
		j.Logger.Infof("batch √ ( size %d )", j.batchHandler.Size())
		return feedWithBatch(j.batchHandler.Size(), j.batchHandler.Batch)
	} else {
		j.Logger.Info("batch ×")
		return feedWithBatch(1, func(ctx context.Context, mash []interface{}) (interface{}, error) {
			return mash[0], nil
		})
	}
}

func (j *Job) chew(ctx context.Context, in <-chan interface{}) <-chan Done {
	type actFn func(ctx context.Context, p interface{}) (r interface{}, e error)

	chewWithAction := func(act actFn) <-chan Done {
		out := make(chan Done, j.workers)
		waiter := make(chan bool, j.workers)
		for i := 0; i < j.workers; i++ {
			go func(in <-chan interface{}, out chan<- Done, waiter chan<- bool) {
				for para := range in {
					if done, ok := para.(Done); ok {
						j.Logger.Infof("√ action succeed, pipe error ( %s ) through", done.E.Error())
						out <- done // batch error
					} else {
						ret, err := act(ctx, para)
						if err != nil {
							j.Logger.Errorf("× action failed ( %+v, %s )", para, err.Error())
							out <- Done{
								para,
								ret, // be tolerant with error, keep last successful ret
								newError(Action, fmt.Errorf("( %+v, %s )", para, err.Error()))}
						} else {
							j.Logger.Infof("√ action succeed ( %+v, %+v)", para, ret)
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
	if j.actionHandler != nil {
		j.Logger.Infof("action √ ( workers %d )", j.workers)
		return chewWithAction(j.actionHandler.Act)
	} else {
		j.Logger.Infof("action ×")
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
			if r.E != nil && j.errorHandler != nil {
				j.errorHandler.OnError(r.E)
			}
			results = append(results, r)
		}
		output <- results
	}(out, output)
	return output
}

func (j *Job) run(ctx context.Context, with []interface{}) []Done {
	return <-j.digest(ctx, j.chew(ctx, j.feed(ctx, with)))
}

func (j *Job) BatchHandler(bh batchHandler) *Job {
	j.batchHandler = bh
	return j
}

func (j *Job) ActionHandler(ah actionHandler) *Job {
	j.actionHandler = ah
	return j
}

func (j *Job) RetryHandler(rh retryHandler) *Job {
	j.retryHandler = rh
	return j
}

func (j *Job) ErrorHandler(eh errorHandler) *Job {
	j.errorHandler = eh
	return j
}

func (j *Job) Run(ctx context.Context, with []interface{}) []Done {
	child, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	var finalAllDone []Done
	allDone := j.run(child, with)
	if j.retryHandler == nil {
		j.Logger.Info("retry ×")
		finalAllDone = allDone
	} else {
		retries := 1
		for {
			var batchRetries []interface{}
			var batchFailed []Done
			var actionRetries []interface{}
			var actionFailed []Done
			for _, done := range allDone {
				if j.retryHandler.Worth(done) {
					if e, ok := done.E.(*Error); ok && e.hook == Batch {
						if groupedPara, isArray := done.P.([]interface{}); isArray {
							batchRetries = append(batchRetries, groupedPara...)
							for _, para := range groupedPara {
								batchFailed = append(batchFailed, Done{para, nil, done.E})
							}
						} else {
							j.Logger.Error("× cast para failed, skip retry")
						}
					} else {
						actionRetries = append(actionRetries, done.P)
						actionFailed = append(actionFailed, done)
					}
				} else { // not worth retrying
					finalAllDone = append(finalAllDone, done)
				}
			}

			if j.retryHandler.Forgo() {
				if len(batchFailed) > 0 {
					finalAllDone = append(finalAllDone, batchFailed...)
				}
				if len(actionFailed) > 0 {
					finalAllDone = append(finalAllDone, actionFailed...)
				}
				j.Logger.Infof("√ retry ended ( %d batch failures, %d action failures )",
					len(batchFailed), len(actionFailed))
				break
			} else {
				allDone = []Done{}
				if len(actionRetries) > 0 {
					j.Logger.Infof("√ retry ( %d ) on ( %d action failures )", retries, len(actionRetries))
					allDone = append(allDone, NewJob(j.Logger, j.workers).ActionHandler(
						j.actionHandler).run(child, actionRetries)...)
				}
				if len(batchRetries) > 0 {
					j.Logger.Infof("√ retry ( %d ) on ( %d batch failures )", retries, len(batchRetries))
					allDone = append(allDone, NewJob(j.Logger, j.workers).BatchHandler(j.batchHandler).ActionHandler(
						j.actionHandler).run(child, batchRetries)...)
				}
				retries++
			}
		}
	}
	j.Logger.Infof("√ finished with ( %+v )", finalAllDone)
	return finalAllDone
}

func NewJob(logger logging.Logger, workers int) *Job {
	if workers <= 0 {
		workers = runtime.NumCPU() * 64
	}
	j := &Job{logger, workers,
		nil, nil,
		nil, nil}
	return j
}
