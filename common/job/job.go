package job

import (
	"context"
	"fmt"
	"runtime"

	"github.com/samwooo/bolsa/common/logging"
)

func (j *Job) drain(ctx context.Context, s Supplier) <-chan []interface{} {
	in := make(chan []interface{}, j.workers)
	go func() {
		for !s.Empty() {
			if data, err := s.Drain(ctx); err != nil {
				j.Logger.Errorf("× drain failed ( %s )", err.Error())
			} else {
				j.Logger.Infof("√ drain succeed ( %+v )", data)
				in <- []interface{}{data}
			}
		}
		close(in)
	}()

	if j.batchStrategy == nil {
		j.Logger.Info("drain batch ×")
		return in
	} else {
		j.Logger.Infof("drain batch √ ( size %d )", j.batchStrategy.Size())
		out := make(chan []interface{}, j.workers)
		go func() {
			var batched []interface{}
			for d := range in {
				batched = append(batched, d...)
				if len(batched) == j.batchStrategy.Size() {
					j.Logger.Infof("√ drain batch succeed ( %+v )", batched)
					out <- batched
					batched = []interface{}{}
				}
			}
			if len(batched) > 0 {
				j.Logger.Infof("√ drain batch succeed ( %+v )", batched)
				out <- batched
				batched = []interface{}{}
			}
			close(out)
		}()
		return out
	}
}

func (j *Job) feed(ctx context.Context, groupedDataCh <-chan []interface{}) <-chan interface{} {
	type reduceFn func(context.Context, []interface{}) (interface{}, error)

	feedWithBatch := func(reduce reduceFn) <-chan interface{} {
		in := make(chan interface{}, j.workers)
		waiter := make(chan bool, j.workers)

		for k := 0; k < j.workers; k++ {
			go func() {
				for groupedData := range groupedDataCh {
					if data, err := reduce(ctx, groupedData); err != nil {
						j.Logger.Errorf("× batch failed ( %+v, %s )", groupedData, err.Error())
						in <- Done{
							groupedData,
							data,
							newError(typeBatch, fmt.Errorf("( %+v, %s )", groupedData, err.Error()))}
					} else {
						j.Logger.Infof("√ batch succeed ( %+v, %+v )", groupedData, data)
						in <- data
					}
				}
				waiter <- true
			}()
		}

		go func() {
			for i := 0; i < j.workers; i++ {
				<-waiter
			}
			close(in)
		}()
		return in
	}

	if j.batchStrategy != nil {
		j.Logger.Infof("batch √ ( size %d )", j.batchStrategy.Size())
		return feedWithBatch(j.batchStrategy.Reduce)
	} else {
		j.Logger.Info("batch ×")
		return feedWithBatch(func(ctx context.Context, mash []interface{}) (interface{}, error) {
			return mash[0], nil
		})
	}
}

func (j *Job) chew(ctx context.Context, in <-chan interface{}) <-chan Done {
	type workFn func(ctx context.Context, p interface{}) (r interface{}, e error)

	chewWithLabor := func(work workFn) <-chan Done {
		out := make(chan Done, j.workers)
		waiter := make(chan bool, j.workers)

		for i := 0; i < j.workers; i++ {
			go func() {
				for para := range in {
					if done, ok := para.(Done); ok {
						j.Logger.Infof("√ labor succeed, pipe error ( %s ) through", done.E.Error())
						out <- done // batch error
					} else {
						ret, err := work(ctx, para)
						if err != nil {
							j.Logger.Errorf("× labor failed ( %+v, %s )", para, err.Error())
							out <- Done{
								para,
								ret, // be tolerant with error, keep last successful ret
								newError(typeLabor, fmt.Errorf("( %+v, %s )", para, err.Error()))}
						} else {
							j.Logger.Infof("√ labor succeed ( %+v, %+v)", para, ret)
							out <- Done{para, ret, nil}
						}
					}
				}
				waiter <- true
			}()
		}

		go func() {
			for i := 0; i < j.workers; i++ {
				<-waiter
			}
			close(out)
		}()
		return out
	}

	if j.laborStrategy != nil {
		j.Logger.Infof("labor √ ( workers %d )", j.workers)
		return chewWithLabor(j.laborStrategy.Work)
	} else {
		j.Logger.Infof("labor ×")
		return chewWithLabor(func(ctx context.Context, para interface{}) (interface{}, error) {
			return para, nil
		})
	}
}

func (j *Job) digest(ctx context.Context, in <-chan Done) <-chan []Done {
	output := make(chan []Done)
	go func() {
		var results []Done
		for r := range in {
			if r.E != nil && j.errorStrategy != nil {
				j.errorStrategy.OnError(r.E)
			}
			results = append(results, r)
		}
		output <- results
	}()
	return output
}

func (j *Job) runWithSupplier(ctx context.Context, s Supplier) []Done {
	return <-j.digest(ctx, j.chew(ctx, j.feed(ctx, j.drain(ctx, s))))
}

func (j *Job) BatchStrategy(bh batchStrategy) *Job {
	j.batchStrategy = bh
	return j
}

func (j *Job) LaborStrategy(lh laborStrategy) *Job {
	j.laborStrategy = lh
	return j
}

func (j *Job) RetryStrategy(rh retryStrategy) *Job {
	j.retryStrategy = rh
	return j
}

func (j *Job) ErrorStrategy(eh errorStrategy) *Job {
	j.errorStrategy = eh
	return j
}

func (j *Job) Run(ctx context.Context, s Supplier) []Done {
	child, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	var finalAllDone []Done
	allDone := j.runWithSupplier(child, s)
	if j.retryStrategy == nil {
		j.Logger.Info("retry ×")
		finalAllDone = allDone
	} else {
		retries := 1
		for {
			var batchRetries []interface{}
			var batchFailed []Done
			var laborRetries []interface{}
			var laborFailed []Done
			for _, done := range allDone {
				if j.retryStrategy.Worth(done) {
					if e, ok := done.E.(*Error); ok && e.st == typeBatch {
						if groupedPara, isArray := done.P.([]interface{}); isArray {
							batchRetries = append(batchRetries, groupedPara...)
							for _, para := range groupedPara {
								batchFailed = append(batchFailed, Done{para, nil, done.E})
							}
						} else {
							j.Logger.Error("× cast para failed, skip retry")
						}
					} else {
						laborRetries = append(laborRetries, done.P)
						laborFailed = append(laborFailed, done)
					}
				} else { // not worth retrying
					finalAllDone = append(finalAllDone, done)
				}
			}

			if j.retryStrategy.Forgo() {
				if len(batchFailed) > 0 {
					finalAllDone = append(finalAllDone, batchFailed...)
				}
				if len(laborFailed) > 0 {
					finalAllDone = append(finalAllDone, laborFailed...)
				}
				j.Logger.Infof("√ retry ended ( %d batch failures, %d labor failures )",
					len(batchFailed), len(laborFailed))
				break
			} else {
				runJobWithData := func(ctx context.Context, j *Job, data []interface{}) []Done {
					return j.runWithSupplier(ctx, NewDataSupplier(data))
				}

				allDone = []Done{}
				if len(laborRetries) > 0 {
					j.Logger.Infof("√ retry ( %d ) on ( %d labor failures )", retries, len(laborRetries))
					allDone = append(allDone, runJobWithData(child, NewJob(j.Logger, j.workers).LaborStrategy(
						j.laborStrategy), laborRetries)...)
				}
				if len(batchRetries) > 0 {
					j.Logger.Infof("√ retry ( %d ) on ( %d batch failures )", retries, len(batchRetries))
					allDone = append(allDone, runJobWithData(child, NewJob(j.Logger, j.workers).BatchStrategy(
						j.batchStrategy).LaborStrategy(j.laborStrategy), batchRetries)...)
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
