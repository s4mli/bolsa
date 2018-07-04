package task

import "context"

type Worker struct {
	Ctx      context.Context
	CancelFn context.CancelFunc
}

type workFn func(context.Context, interface{}) error
type Task struct {
	work    workFn
	workers []Worker
	count   int
}

func incWorkers(count int) {

}

func decWorkers(count int) {

}

func (t *Task) Do(ctx context.Context, p interface{}) error {
	for i := 0; i < t.count; i++ {
		go func() {
			if err := t.work(ctx, p); err != nil {
				if err == context.Canceled {

				}
			}
		}()
	}
}

func (t *Task) Scale(workers int) {

}

func NewTask(ctx context.Context, work workFn, count int) *Task {
	var workers []Worker
	ctx, cancelFn := context.WithCancel(ctx)
	for i := 0; i < count; i++ {
		workers = append(workers, Worker{ctx, cancelFn})
	}
	return &Task{work, workers, count}
}
