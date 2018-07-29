package job

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/samwooo/bolsa/common/logging"
)

func (j *Job) worthRetry(d Done) bool {
	if j.retryStrategy == nil {
		return false
	} else {
		return j.retryStrategy.Worth(d)
	}
}

func (j *Job) retryLimit() int {
	if j.retryStrategy == nil || j.retryStrategy.Limit() < 1 {
		return 0
	} else {
		return j.retryStrategy.Limit()
	}
}

func (j *Job) drain(ctx context.Context, input <-chan Done) <-chan Done {
	return NewTask(j.Logger, fmt.Sprintf("%s-drain", j.name),
		func(ctx context.Context, d Done) (Done, bool) {
			j.Logger.Debugf("✔ job %s drain succeed ( %+v )", j.name, d)
			// R = P for feed
			return NewDone(nil, d.P, d.E, d.retries, d.D, d.Key), true
		}).Run(ctx, j.workers, input)
}

func (j *Job) chew(ctx context.Context, input <-chan Done) <-chan Done {
	type worker func(ctx context.Context, p interface{}) (r interface{}, e error)
	chewWithLabor := func(workers int, input <-chan Done, work worker) <-chan Done {
		return NewTask(j.Logger, fmt.Sprintf("%s-chew", j.name),
			func(ctx context.Context, d Done) (Done, bool) {
				if data, err := work(ctx, d.P); err != nil {
					j.Logger.Warnf("✗ job %s chew failed ( %+v, %s )", j.name, d, err.Error())
					laborError := newError(typeLabor, fmt.Errorf("( %+v, %s )", d.P, err.Error()))
					// P is P & R is R for digest
					laborFailed := NewDone(d.P, data, laborError, d.retries, d.D, d.Key)
					if j.worthRetry(laborFailed) && d.retries < j.retryLimit() {
						// R = P for drain
						lr := NewDone(nil, d.P, laborError, d.retries+1, d.D, d.Key)
						j.Logger.Warnf("✔ job %s retry chew failure ( %+v )", j.name, lr)
						j.feeder.Retry(lr)
						return laborFailed, laborFailed.retries > j.retryLimit() || j.feeder.Closed()
					} else {
						return laborFailed, true
					}
				} else {
					j.Logger.Debugf("✔ job %s chew succeed ( %+v, %+v)", j.name, d.P, data)
					// R = P for digest
					return NewDone(nil, data, nil, d.retries, d.D, d.Key), true
				}
			}).Run(ctx, workers, input)
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

func (j *Job) digest(ctx context.Context, inputs ...<-chan Done) <-chan Done {
	merge := func(inputs ...<-chan Done) <-chan Done {
		var wg sync.WaitGroup
		wg.Add(len(inputs))
		output := make(chan Done)
		for _, in := range inputs {
			go func(in <-chan Done) {
				for d := range in {
					if d.E != nil {
						if j.errorStrategy != nil {
							j.errorStrategy.OnError(d)
						}
					}
					output <- d
				}
				wg.Done()
			}(in)
		}
		go func() {
			wg.Wait()
			close(output)
		}()
		return output
	}
	return merge(inputs...)
}

func (j *Job) description() string {
	return fmt.Sprintf(
		"\n   ⬨ Job - %s\n"+
			"      ⬨ Feeder          %s\n"+
			"      ⬨ Workers         %d\n"+
			"      ⬨ LaborStrategy   %s\n"+
			"      ⬨ RetryStrategy   %s\n",
		j.name,
		j.feeder.Name(),
		j.workers,

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

func (j *Job) Run(ctx context.Context) *sync.Map {
	j.Logger.Info(j.description())
	ready := make(chan *sync.Map)
	go func() {
		var result sync.Map
		for r := range j.digest(ctx, j.chew(ctx, j.drain(ctx, j.feeder.Adapt()))) {
			if v, existing := result.Load(r.Key); existing {
				if d, _ := v.(Done); d.retries < r.retries {
					result.Store(r.Key, r)
				}
			} else {
				result.Store(r.Key, r)
			}
		}
		ready <- &result
		close(ready)
	}()
	return <-ready
}

func NewJob(logger logging.Logger, name string, workers int, feeder *Feeder) *Job {
	if workers <= 0 {
		workers = runtime.NumCPU() * 64
	}
	j := &Job{logger, name, workers,
		feeder, nil, nil, nil}
	return j
}
