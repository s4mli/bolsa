package job

import (
	"context"
	"fmt"

	"github.com/samwooo/bolsa/common/logging"
)

type Done struct {
	P, R interface{}
	E    error
}

func (d *Done) String() string {
	return fmt.Sprintf(" * Params: %+v\n * Result: ( %+v , %+v )", d.P, d.R, d.E)
}

///////////
// Task //
type task func(ctx context.Context, d Done) Done
type Task struct {
	logger  logging.Logger
	name    string
	workers int
	batch   int
	task    task
}

func (t *Task) Run(ctx context.Context, input <-chan Done) <-chan Done {
	waitAndExitGracefully := func(workers int, ch chan<- Done, waitress <-chan bool) {
		go func() {
			for i := 0; i < workers; i++ {
				<-waitress
			}
			close(ch)
		}()
	}

	runTask := func(workers, batch int, input <-chan Done, output chan<- Done) <-chan bool {

		runWithBatch := func(batch int, input <-chan Done, output chan<- Done) {

			fillWithBatch := func(batched []Done, output chan<- Done) {
				var rs []interface{}
				for _, b := range batched {
					if b.R != nil && b.E == nil {
						// last R as a P
						rs = append(rs, b.R)
					}
				}

				if len(rs) > 0 {
					r := t.task(ctx, Done{rs, nil, nil})
					t.logger.Debugf("√ %s task done ( %+v )", t.name, r)
					output <- r
				} else {
					// error
					t.logger.Debugf("√ %s task skipped ( %+v )", t.name, batched[0])
					output <- batched[0]
				}
			}

			var batched []Done
			for {
				if d, more := <-input; more {
					if d.E != nil || d.R == nil {
						t.logger.Debugf("√ %s task skipped ( %+v )", t.name, d)
						batched = append(batched, d)
						fillWithBatch(batched, output)
						batched = []Done{}
					} else {
						batched = append(batched, d)
						if len(batched) == batch {
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
				batched = []Done{}
			}
		}

		runWithoutBatch := func(input <-chan Done, output chan<- Done) {

			fillWithoutBatch := func(d Done, output chan<- Done) {
				if d.E != nil || d.R == nil {
					t.logger.Debugf("√ %s task skipped ( %+v )", t.name, d)
					output <- d
				} else {
					// last R as a P
					r := t.task(ctx, Done{d.R, nil, nil})
					t.logger.Debugf("√ %s task done ( %+v )", t.name, r)
					output <- r
				}
			}

			for d := range input {
				fillWithoutBatch(d, output)
			}
		}

		waitress := make(chan bool, t.workers)
		for i := 0; i < t.workers; i++ {
			go func() {
				if batch > 1 {
					runWithBatch(batch, input, output)
				} else {
					runWithoutBatch(input, output)
				}
				waitress <- true
			}()
		}
		return waitress
	}

	t.logger.Debugf("❋ kickoff %s task ( workers %d, batch %d )", t.name, t.workers, t.batch)
	output := make(chan Done, t.workers)
	waitAndExitGracefully(t.workers, output, runTask(t.workers, t.batch, input, output))
	return output
}

func NewTask(logger logging.Logger, name string, workers, batch int, task task) *Task {
	return &Task{logger, name, workers, batch, task}
}
