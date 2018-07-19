package job

import (
	"context"
	"fmt"
	"sync"
)

func (j *Job) batchSize() int {
	if j.batchStrategy == nil || j.batchStrategy.Size() <= 1 {
		return 1
	} else {
		return j.batchStrategy.Size()
	}
}

func (j *Job) drain(ctx context.Context, input <-chan Done) <-chan Done {
	return NewTask(j.Logger, "drain",
		func(ctx context.Context, d Done) Done {
			j.Logger.Debugf("✔ %s drain succeed ( %+v )", j.name, d)
			return newDone(nil, d.P, nil)
		}, nil).Run(ctx, j.workers, j.batchSize(), input)
}

func (j *Job) feed(ctx context.Context, input <-chan Done) <-chan Done {
	type reducer func(context.Context, []interface{}) (interface{}, error)
	feedWithReduce := func(workers int, input <-chan Done, reduce reducer) <-chan Done {
		return NewTask(j.Logger, "reduce",
			func(ctx context.Context, d Done) Done {
				var batchedData []interface{}
				if j.batchSize() <= 1 {
					batchedData = append(batchedData, d.P)
				} else {
					batchedData, _ = d.P.([]interface{})
				}
				if data, err := reduce(ctx, batchedData); err != nil {
					j.Logger.Errorf("✗ %s reduce failed ( %+v, %s )", j.name, batchedData, err.Error())
					return newDone(batchedData, batchedData,
						newError(typeBatch, fmt.Errorf("( %+v, %s )", batchedData, err.Error())))
				} else {
					j.Logger.Debugf("✔ %s reduce succeed ( %+v, %+v )", j.name, batchedData, data)
					return newDone(batchedData, data, nil)
				}
			}, nil).Run(ctx, workers, 1, input)
	}
	// want Reduce to be executed even batch size is 1
	if j.batchStrategy != nil {
		return feedWithReduce(j.workers, input, j.batchStrategy.Reduce)
	} else {
		return feedWithReduce(j.workers, input,
			func(ctx context.Context, batchedData []interface{}) (interface{}, error) {
				return batchedData[0], nil
			})
	}
}

func (j *Job) chew(ctx context.Context, input <-chan Done) <-chan Done {
	type worker func(ctx context.Context, p interface{}) (r interface{}, e error)
	chewWithLabor := func(workers int, input <-chan Done, work worker) <-chan Done {
		return NewTask(j.Logger, "labor",
			func(ctx context.Context, d Done) Done {
				if d.E != nil {
					j.Logger.Infof("✔ %s labor skipped, pipe batch failure ( %s )", j.name, d.E.Error())
					return d
				} else {
					if data, err := work(ctx, d.P); err != nil {
						j.Logger.Errorf("✗ %s labor failed ( %+v, %s )", j.name, d.P, err.Error())
						return newDone(d.P, data,
							newError(typeLabor, fmt.Errorf("( %+v, %s )", d.P, err.Error())))
					} else {
						j.Logger.Debugf("✔ %s labor succeed ( %+v, %+v)", j.name, d.P, data)
						return newDone(d.P, data, nil)
					}
				}
			}, nil).Run(ctx, workers, 1, input)
	}
	if j.laborStrategy != nil {
		return chewWithLabor(j.workers, input, j.laborStrategy.Work)
	} else {
		return chewWithLabor(j.workers, input,
			func(ctx context.Context, para interface{}) (interface{}, error) {
				return para, nil
			})
	}
}

func (j *Job) digest(ctx context.Context, inputs ...<-chan Done) <-chan []Done {
	merge := func(inputs ...<-chan Done) <-chan Done {
		var wg sync.WaitGroup
		wg.Add(len(inputs))
		output := make(chan Done)
		for _, input := range inputs {
			go func(input <-chan Done) {
				for in := range input {
					output <- in
				}
				wg.Done()
			}(input)
		}
		go func() {
			wg.Wait()
			close(output)
		}()
		return output
	}

	out := make(chan []Done)
	go func() {
		var results []Done
		for r := range merge(inputs...) {
			if r.E != nil {
				if j.errorStrategy != nil {
					j.errorStrategy.OnError(r)
				}
			}
			results = append(results, r)
		}
		out <- results
		close(out)
	}()
	return out
}

func (j *Job) kickoff(ctx context.Context, input <-chan Done) <-chan Done {
	return j.chew(ctx, j.feed(ctx, j.drain(ctx, input)))
}

func (j *Job) run(ctx context.Context, f feeder) []Done {
	description := func() string {
		return fmt.Sprintf(
			"\n   ⬨ Job - %s\n"+
				"      ⬨ Feeder          %s\n"+
				"      ⬨ Workers         %d\n"+
				"      ⬨ BatchStrategy   %s\n"+
				"      ⬨ LaborStrategy   %s\n"+
				"      ⬨ RetryStrategy   %s\n",
			j.name, f.Name(), j.workers,
			func() string {
				if j.batchStrategy != nil {
					return fmt.Sprintf("✔ ( %d )", j.batchStrategy.Size())
				} else {
					return "✗"
				}
			}(),
			func() string {
				if j.laborStrategy != nil {
					return "✔"
				} else {
					return "✗"
				}
			}(),
			func() string {
				if j.retryStrategy != nil {
					return fmt.Sprintf("✔ ( %d )", j.retryStrategy.Limit())
				} else {
					return "✗"
				}
			}(),
		)
	}
	j.Logger.Info(description())
	return <-j.digest(ctx, j.kickoff(ctx, f.Adapt()))
}

func (j *Job) retry(ctx context.Context, jobDone []Done, quit <-chan bool) []Done {
	fanOut := func(jobDone []Done) (succeed, batchFailed, laborFailed []Done,
		batchRetries, laborRetries []interface{}) {

		bypassRetry := func(r Done) error {
			if e, ok := r.E.(*Error); ok && e.T == typeBatch {
				if groupedPara, isArray := r.P.([]interface{}); isArray {
					batchRetries, batchFailed = append(batchRetries, groupedPara...), func() []Done {
						for _, para := range groupedPara {
							batchFailed = append(batchFailed, newDone(para, nil, r.E))
						}
						return batchFailed
					}()
					return nil
				} else {
					return fmt.Errorf("✗ %s cast ( %+v ) failed, skip retry", j.name, r.P)
				}
			} else {
				laborRetries, laborFailed = append(laborRetries, r.P), append(laborFailed, r)
				return nil
			}
		}
		for _, done := range jobDone {
			if j.retryStrategy.Worth(done) {
				if err := bypassRetry(done); err != nil {
					j.Logger.Error(err)
					succeed = append(succeed, done)
				}
			} else {
				succeed = append(succeed, done)
			}
		}
		return
	}

	doRetry := func(retries int, batchRetries, laborRetries []interface{}) []Done {
		runJob := func(ctx context.Context, j *Job, data []interface{}) []Done {
			return j.run(ctx, NewDataFeeder(data))
		}
		var retryDone []Done
		if len(laborRetries) > 0 {
			retryDone = append(retryDone, runJob(ctx, NewJob(j.Logger, fmt.Sprintf("%sLaborRetry%d",
				j.name, retries), j.workers).LaborStrategy(j.laborStrategy), laborRetries)...)
		}
		if len(batchRetries) > 0 {
			retryDone = append(retryDone, runJob(ctx, NewJob(j.Logger, fmt.Sprintf("%sBatchRetry%d",
				j.name, retries), j.workers).BatchStrategy(j.batchStrategy).LaborStrategy(j.laborStrategy),
				batchRetries)...)
		}
		return retryDone
	}

	result := make(chan []Done)
	go func(result chan<- []Done) {
		retries, stop := 1, false
		var allDone []Done
		for {
			select {
			case <-quit:
				stop = true
			default:
			}

			succeed, batchFailed, laborFailed, batchRetries, laborRetries := fanOut(jobDone)
			allDone = append(allDone, succeed...)
			if retries < j.retryStrategy.Limit() && !stop {
				j.Logger.Infof("✔ %s retry ( %d batch failures, %d labor failures )", j.name,
					len(batchRetries), len(laborRetries))
				jobDone = doRetry(retries, batchRetries, laborRetries)
				retries++
			} else {
				j.Logger.Infof("✔ %s retry succeed ( %d times, %d batch failures, %d labor failures )",
					j.name, retries-1, len(batchFailed), len(laborFailed))
				allDone = append(allDone, batchFailed...)
				allDone = append(allDone, laborFailed...)
				break
			}
		}
		result <- allDone
		close(result)
	}(result)
	return <-result
}
