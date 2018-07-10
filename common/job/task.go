package job

import (
	"context"
	"fmt"

	"github.com/samwooo/bolsa/common/logging"
)

///////////
// Task //
type task func(ctx context.Context, d Done) Done
type Task struct {
	logger logging.Logger
	name   string
	task   task
}

func (t *Task) Run(ctx context.Context, workers, inputBatch int, input <-chan Done) <-chan Done {
	waitAndExitGracefully := func(workers int, ch chan<- Done, waitress <-chan bool) {
		go func() {
			for i := 0; i < workers; i++ {
				<-waitress
			}
			close(ch)
		}()
	}

	runTask := func(workers, inputBatch int, input <-chan Done, output chan<- Done) <-chan bool {

		runWithBatch := func(inputBatch int, input <-chan Done, output chan<- Done) {

			fillWithBatch := func(batched []Done, output chan<- Done) {
				var rs []interface{}
				for _, b := range batched {
					if b.R != nil && b.E == nil {
						// last R as a P
						rs = append(rs, b.R)
					}
				}

				if len(rs) > 0 {
					r := t.task(ctx, newDone(rs, nil, nil))
					t.logger.Debugf("✔ %s task done ( %+v )", t.name, r)
					output <- r
				} else {
					// error
					t.logger.Debugf("✔ %s task skipped ( %+v )", t.name, batched[0])
					output <- batched[0]
				}
			}

			var batched []Done
			for {
				if d, more := <-input; more {
					if d.E != nil || d.R == nil {
						t.logger.Debugf("✔ %s task skipped ( %+v )", t.name, d)
						batched = append(batched, d)
						fillWithBatch(batched, output)
						batched = []Done{}
					} else {
						batched = append(batched, d)
						if len(batched) == inputBatch {
							fillWithBatch(batched, output)
							batched = []Done{}
						}
					}
				} else {
					break
				}
			}
			if len(batched) > 0 {
				fillWithBatch(batched, output)
			}
		}

		runWithoutBatch := func(input <-chan Done, output chan<- Done) {

			fillWithoutBatch := func(d Done, output chan<- Done) {
				if d.E != nil || d.R == nil {
					t.logger.Debugf("✔ %s task skipped ( %+v )", t.name, d)
					output <- d
				} else {
					r := t.task(ctx, newDone(d.R, nil, nil))
					t.logger.Debugf("✔ %s task done ( %+v )", t.name, r)
					output <- r
				}
			}

			for d := range input {
				fillWithoutBatch(d, output)
			}
		}

		waitress := make(chan bool, workers)
		for i := 0; i < workers; i++ {
			go func() {
				if inputBatch > 1 {
					runWithBatch(inputBatch, input, output)
				} else {
					runWithoutBatch(input, output)
				}
				waitress <- true
			}()
		}
		return waitress
	}

	t.logger.Debug(fmt.Sprintf("\n   ⬨ Task - %s\n"+
		"      ⬨ Workers    %d\n"+
		"      ⬨ Batch      %d\n", t.name, workers, inputBatch))
	output := make(chan Done, workers)
	waitAndExitGracefully(workers, output, runTask(workers, inputBatch, input, output))
	return output
}

func NewTask(logger logging.Logger, name string, task task) *Task {
	return &Task{logger, name, task}
}
