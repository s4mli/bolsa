package task

import (
	"context"
	"fmt"
	"time"

	"github.com/samwooo/bolsa/common/job/model"
	"github.com/samwooo/bolsa/common/logging"
)

///////////
// Task //
type task func(ctx context.Context, d model.Done) (result model.Done, ok bool)
type Task struct {
	logger logging.Logger
	name   string
	task   task
}

func (t *Task) run(ctx context.Context, input <-chan model.Done, output chan<- model.Done) {
	apply := func(d model.Done, output chan<- model.Done) {
		if r, ok := t.task(ctx, model.NewDone(d.R, nil, d.E, d.Retries, d.D, d.Key)); ok {
			t.logger.Debugf("✔ task %s done ( %+v )", t.name, r)
			output <- r
		} else {
			t.logger.Infof("✔ task %s skipped ( %+v )", t.name, r)
		}
	}

	for {
		select {
		case d, more := <-input:
			if more {
				if d.R == nil {
					t.logger.Warnf("✔ task %s skipped ( %+v, R ? )", t.name, d)
				} else {
					apply(d, output)
				}
			} else {
				t.logger.Debugf("✔ task %s exit ...", t.name)
				return
			}
		default:
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (t *Task) Run(ctx context.Context, workers int, input <-chan model.Done) <-chan model.Done {
	exitGracefully := func(workers int, output chan<- model.Done, waitress <-chan bool) {
		go func() {
			for i := 0; i < workers; i++ {
				<-waitress
			}
			close(output)
		}()
	}

	runTask := func(workers int, input <-chan model.Done, output chan<- model.Done) <-chan bool {
		waitress := make(chan bool, workers)
		for i := 0; i < workers; i++ {
			go func() {
				t.run(ctx, input, output)
				waitress <- true
			}()
		}
		return waitress
	}

	t.logger.Debug(fmt.Sprintf("\n   ⬨ Task - %s\n"+
		"      ⬨ Workers    %d\n", t.name, workers))
	output := make(chan model.Done, workers)
	exitGracefully(workers, output, runTask(workers, input, output))
	return output
}

func NewTask(logger logging.Logger, name string, task task) *Task {
	return &Task{logger, name, task}
}
