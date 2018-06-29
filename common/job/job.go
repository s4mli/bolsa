package job

import (
	"context"
	"fmt"
	"runtime"

	"github.com/samwooo/bolsa/common/logging"
)

func (j *Job) drain(ctx context.Context, s supplier) <-chan []interface{} {

	waitAndExitGracefully := func(workers int, ch chan<- []interface{}, done <-chan bool) {
		go func() {
			for i := 0; i < workers; i++ {
				<-done
			}
			close(ch)
		}()
	}

	doDrain := func(workers int, in chan<- []interface{}) <-chan bool {
		done := make(chan bool, workers)
		for i := 0; i < workers; i++ {
			go func() {
				for {
					if data, ok := s.Drain(ctx); ok {
						j.Logger.Infof("√ drain succeed ( %+v )", data)
						in <- []interface{}{data}
					} else {
						break
					}
				}
				done <- true
			}()
		}
		return done
	}

	doBatch := func(workers int, in <-chan []interface{}, out chan<- []interface{}) <-chan bool {
		done := make(chan bool, workers)
		for i := 0; i < workers; i++ {
			go func() {
				var batched []interface{}
				for d := range in {
					batched = append(batched, d...)
					if len(batched) == j.batchStrategy.Size() {
						j.Logger.Infof("√ batch succeed ( %+v )", batched)
						out <- batched
						batched = []interface{}{}
					}
				}
				if len(batched) > 0 {
					j.Logger.Infof("√ batch succeed ( %+v )", batched)
					out <- batched
				}
				done <- true
			}()
		}
		return done
	}

	// Drain in N goroutines
	in := make(chan []interface{}, j.workers)
	waitAndExitGracefully(j.workers, in, doDrain(j.workers, in))

	if j.batchStrategy == nil {
		j.Logger.Info("batch ×")
		return in
	} else {
		j.Logger.Infof("batch √ ( size %d )", j.batchStrategy.Size())
		// Batch in N goroutines
		out := make(chan []interface{}, j.workers)
		waitAndExitGracefully(j.workers, out, doBatch(j.workers, in, out))
		return out
	}
}

func (j *Job) feed(ctx context.Context, in <-chan []interface{}) <-chan interface{} {

	waitAndExitGracefully := func(workers int, ch chan<- interface{}, done <-chan bool) {
		go func() {
			for i := 0; i < workers; i++ {
				<-done
			}
			close(ch)
		}()
	}

	type reducer func(context.Context, []interface{}) (interface{}, error)

	doReduce := func(workers int, reduce reducer, in <-chan []interface{}, out chan<- interface{}) <-chan bool {
		done := make(chan bool, workers)
		for i := 0; i < workers; i++ {
			go func() {
				for batchedData := range in {
					if data, err := reduce(ctx, batchedData); err != nil {
						j.Logger.Errorf("× reduce failed ( %+v, %s )", batchedData, err.Error())
						out <- Done{
							batchedData,
							data,
							newError(typeBatch, fmt.Errorf("( %+v, %s )", batchedData, err.Error()))}
					} else {
						j.Logger.Infof("√ reduce succeed ( %+v, %+v )", batchedData, data)
						out <- data
					}
				}
				done <- true
			}()
		}
		return done
	}

	// Reduce in N goroutines
	feedWithReduce := func(workers int, reduce reducer) <-chan interface{} {
		out := make(chan interface{}, workers)
		waitAndExitGracefully(workers, out, doReduce(workers, reduce, in, out))
		return out
	}

	if j.batchStrategy != nil {
		j.Logger.Infof("reduce √ ( size %d )", j.batchStrategy.Size())
		return feedWithReduce(j.workers, j.batchStrategy.Reduce)
	} else {
		j.Logger.Info("reduce ×")
		return feedWithReduce(j.workers,
			func(ctx context.Context, batchedData []interface{}) (interface{}, error) {
				return batchedData[0], nil
			})
	}
}

func (j *Job) chew(ctx context.Context, in <-chan interface{}) <-chan Done {

	waitAndExitGracefully := func(workers int, in chan<- Done, waiter <-chan bool) {
		go func() {
			for i := 0; i < workers; i++ {
				<-waiter
			}
			close(in)
		}()
	}

	type worker func(ctx context.Context, p interface{}) (r interface{}, e error)

	doWork := func(workers int, work worker, in <-chan interface{}, out chan<- Done) <-chan bool {
		done := make(chan bool, workers)
		for i := 0; i < j.workers; i++ {
			go func() {
				for para := range in {
					if batchDone, ok := para.(Done); ok {
						j.Logger.Infof("√ labor succeed, pipe error ( %s ) through", batchDone.E.Error())
						out <- batchDone // batch error
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
				done <- true
			}()
		}
		return done
	}

	// Work in N goroutines
	chewWithLabor := func(workers int, work worker) <-chan Done {
		out := make(chan Done, workers)
		waitAndExitGracefully(j.workers, out, doWork(workers, work, in, out))
		return out
	}

	if j.laborStrategy != nil {
		j.Logger.Infof("labor √ ( workers %d )", j.workers)
		return chewWithLabor(j.workers, j.laborStrategy.Work)
	} else {
		j.Logger.Infof("labor ×")
		return chewWithLabor(j.workers,
			func(ctx context.Context, para interface{}) (interface{}, error) {
				return para, nil
			})
	}
}

func (j *Job) digest(ctx context.Context, in <-chan Done) <-chan []Done {
	out := make(chan []Done)
	go func() {
		var results []Done
		for r := range in {
			if r.E != nil && j.errorStrategy != nil {
				j.errorStrategy.OnError(r.E)
			}
			results = append(results, r)
		}
		out <- results
	}()
	return out
}

func (j *Job) runWithSupplier(ctx context.Context, s supplier) []Done {
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

func (j *Job) Run(ctx context.Context, s supplier) []Done {
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
					// Retry labor failures in a New Job without Supplier
					allDone = append(allDone, runJobWithData(child, NewJob(j.Logger, j.workers).LaborStrategy(
						j.laborStrategy), laborRetries)...)
				}
				if len(batchRetries) > 0 {
					j.Logger.Infof("√ retry ( %d ) on ( %d batch failures )", retries, len(batchRetries))
					// Retry batch failures in a New Job without Supplier
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
