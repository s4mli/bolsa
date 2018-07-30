package job

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/samwooo/bolsa/common/job/feeder"
	"github.com/samwooo/bolsa/common/job/model"
	"github.com/samwooo/bolsa/common/job/task"
	"github.com/samwooo/bolsa/common/logging"
)

//////////
// Job //
type Job struct {
	Logger  logging.Logger
	name    string
	workers int
	feeder  *feeder.Feeder
	labor   model.LaborStrategy
	retry   model.RetryStrategy
}

func (j *Job) worthRetry(d model.Done) bool {
	if j.retry == nil {
		return false
	} else {
		return j.retry.Worth(d)
	}
}

func (j *Job) retryLimit() int {
	if j.retry == nil || j.retry.Limit() < 1 {
		return 0
	} else {
		return j.retry.Limit()
	}
}

func (j *Job) drain(ctx context.Context, input <-chan model.Done) <-chan model.Done {
	return task.NewTask(j.Logger, fmt.Sprintf("%s-drain", j.name),
		func(ctx context.Context, d model.Done) (model.Done, bool) {
			j.Logger.Debugf("✔ job %s drain succeed ( %+v )", j.name, d)
			// R = P for feed
			return model.NewDone(nil, d.P, d.E, d.Retries, d.D, d.Key), true
		}).Run(ctx, j.workers, input)
}

func (j *Job) chew(ctx context.Context, input <-chan model.Done) <-chan model.Done {
	type worker func(ctx context.Context, p interface{}) (r interface{}, e error)
	chewWithLabor := func(workers int, input <-chan model.Done, work worker) <-chan model.Done {
		return task.NewTask(j.Logger, fmt.Sprintf("%s-chew", j.name),
			func(ctx context.Context, d model.Done) (model.Done, bool) {
				if data, err := work(ctx, d.P); err != nil {
					j.Logger.Warnf("✗ job %s chew failed ( %+v, %s )", j.name, d, err.Error())
					laborError := model.NewError(model.TypeLabor, fmt.Errorf("( %+v, %s )", d.P, err.Error()))
					// P is P & R is R for digest
					laborFailed := model.NewDone(d.P, data, laborError, d.Retries, d.D, d.Key)
					if j.worthRetry(laborFailed) && d.Retries < j.retryLimit() {
						// R = P for drain
						lr := model.NewDone(nil, d.P, laborError, d.Retries+1, d.D, d.Key)
						j.Logger.Warnf("✔ job %s retry chew failure ( %+v )", j.name, lr)
						j.feeder.Retry(lr)
						return laborFailed, laborFailed.Retries > j.retryLimit() || j.feeder.Closed()
					} else {
						return laborFailed, true
					}
				} else {
					j.Logger.Debugf("✔ job %s chew succeed ( %+v, %+v)", j.name, d.P, data)
					// R = P for digest
					return model.NewDone(nil, data, nil, d.Retries, d.D, d.Key), true
				}
			}).Run(ctx, workers, input)
	}
	if j.labor != nil {
		return chewWithLabor(j.workers, input, j.labor.Work)
	} else {
		return chewWithLabor(j.workers, input,
			func(ctx context.Context, para interface{}) (interface{}, error) {
				return para, nil
			})
	}
}

func (j *Job) digest(ctx context.Context, inputs ...<-chan model.Done) <-chan model.Done {
	merge := func(inputs ...<-chan model.Done) <-chan model.Done {
		var wg sync.WaitGroup
		wg.Add(len(inputs))
		output := make(chan model.Done)
		for _, in := range inputs {
			go func(in <-chan model.Done) {
				for d := range in {
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
			if j.labor != nil {
				return "✔"
			} else {
				return "✗"
			}
		}(),
		func() string {
			if j.retry != nil {
				return fmt.Sprintf("✔ ( %d )", j.retry.Limit())
			} else {
				return "✗"
			}
		}(),
	)
}

func (j *Job) Feeder(f *feeder.Feeder) *Job {
	j.feeder = f
	return j
}

func (j *Job) LaborStrategy(lh model.LaborStrategy) *Job {
	j.labor = lh
	return j
}

func (j *Job) RetryStrategy(rh model.RetryStrategy) *Job {
	j.retry = rh
	return j
}

func (j *Job) Run(ctx context.Context) *sync.Map {
	j.Logger.Info(j.description())
	ready := make(chan *sync.Map)
	go func() {
		var result sync.Map
		for r := range j.digest(ctx, j.chew(ctx, j.drain(ctx, j.feeder.Adapt()))) {
			if v, existing := result.Load(r.Key); existing {
				if d, _ := v.(model.Done); d.Retries < r.Retries {
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

func NewJob(logger logging.Logger, name string, workers int, feeder *feeder.Feeder) *Job {
	if workers <= 0 {
		workers = runtime.NumCPU() * 64
	}
	return &Job{logger, name, workers, feeder, nil, nil}
}
