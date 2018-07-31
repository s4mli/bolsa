package job

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/samwooo/bolsa/job/feeder"
	"github.com/samwooo/bolsa/job/model"
	"github.com/samwooo/bolsa/job/task"
	"github.com/samwooo/bolsa/logging"
)

//////////
// Job //
type Job struct {
	Logger  logging.Logger
	name    string
	workers int
	*feeder.Feeder
	model.LaborStrategy
	model.RetryStrategy
}

func (j *Job) worthRetry(d model.Done) bool {
	if j.RetryStrategy == nil {
		return false
	} else {
		return j.RetryStrategy.Worth(d)
	}
}

func (j *Job) retryLimit() int {
	if j.RetryStrategy == nil || j.RetryStrategy.Limit() < 1 {
		return 0
	} else {
		return j.RetryStrategy.Limit()
	}
}

func (j *Job) drain(input <-chan model.Done) <-chan model.Done {
	return task.NewTask(j.Logger, fmt.Sprintf("%s-drain", j.name),
		func(d model.Done) (model.Done, bool) {
			j.Logger.Debugf("✔ job %s drain succeed ( %+v )", j.name, d)
			// R = P for feed
			return model.NewDone(nil, d.P, d.E, d.Retries, d.D, d.Key), true
		}).Run(j.workers, input)
}

func (j *Job) chew(input <-chan model.Done) <-chan model.Done {
	type worker func(p interface{}) (r interface{}, e error)
	chewWithLabor := func(workers int, input <-chan model.Done, work worker) <-chan model.Done {
		return task.NewTask(j.Logger, fmt.Sprintf("%s-chew", j.name),
			func(d model.Done) (model.Done, bool) {
				if data, err := work(d.P); err != nil {
					j.Logger.Warnf("✗ job %s chew failed ( %+v, %s )", j.name, d, err.Error())
					laborError := model.NewError(model.TypeLabor, fmt.Errorf("( %+v, %s )", d.P, err.Error()))
					// P is P & R is R for digest
					laborFailed := model.NewDone(d.P, data, laborError, d.Retries, d.D, d.Key)
					if j.worthRetry(laborFailed) && d.Retries < j.retryLimit() {
						// R = P for drain
						lr := model.NewDone(nil, d.P, laborError, d.Retries+1, d.D, d.Key)
						j.Logger.Warnf("✔ job %s RetryStrategy chew failure ( %+v )", j.name, lr)
						j.Feeder.Retry(lr)
						return laborFailed, laborFailed.Retries > j.retryLimit() || j.Feeder.Closed()
					} else {
						return laborFailed, true
					}
				} else {
					j.Logger.Debugf("✔ job %s chew succeed ( %+v, %+v)", j.name, d.P, data)
					// R = P for digest
					return model.NewDone(nil, data, nil, d.Retries, d.D, d.Key), true
				}
			}).Run(workers, input)
	}
	if j.LaborStrategy != nil {
		return chewWithLabor(j.workers, input, j.LaborStrategy.Work)
	} else {
		return chewWithLabor(j.workers, input,
			func(para interface{}) (interface{}, error) {
				return para, nil
			})
	}
}

func (j *Job) digest(inputs ...<-chan model.Done) <-chan model.Done {
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
		j.Feeder.Name(),
		j.workers,

		func() string {
			if j.LaborStrategy != nil {
				return "✔"
			} else {
				return "✗"
			}
		}(),
		func() string {
			if j.RetryStrategy != nil {
				return fmt.Sprintf("✔ ( %d )", j.RetryStrategy.Limit())
			} else {
				return "✗"
			}
		}(),
	)
}

func (j *Job) SetFeeder(f *feeder.Feeder) *Job {
	j.Feeder = f
	return j
}

func (j *Job) SetLaborStrategy(lh model.LaborStrategy) *Job {
	j.LaborStrategy = lh
	return j
}

func (j *Job) SetRetryStrategy(rh model.RetryStrategy) *Job {
	j.RetryStrategy = rh
	return j
}

func (j *Job) Run() *sync.Map {
	j.Logger.Info(j.description())
	if j.Feeder == nil {
		return nil
	} else {
		ready := make(chan *sync.Map)
		go func() {
			var result sync.Map
			for r := range j.digest(j.chew(j.drain(j.Feeder.Adapt()))) {
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
}

func NewJob(logger logging.Logger, name string, workers int, feeder *feeder.Feeder) *Job {
	if workers <= 0 {
		workers = runtime.NumCPU() * 64
	}
	if feeder == nil {
		logger.Error("unable to initialise a job without a feeder!")
		return nil
	} else {
		return &Job{logger, name, workers,
			feeder, nil, nil}
	}
}
