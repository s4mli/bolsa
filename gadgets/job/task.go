package job

import (
	"github.com/samwooo/bolsa/gadgets/logging"
	"github.com/samwooo/bolsa/gadgets/queue"
)

type Task struct {
	a      Action
	cs     ContextStreamer
	logger logging.Logger
}

func (t *Task) feed(contexts []Context) <-chan Context {
	contextsCount := len(contexts)
	in := make(chan Context, contextsCount)
	go func(in chan<- Context) {
		for _, c := range contexts {
			in <- c
		}
		close(in)
	}(in)
	return in
}

func (t *Task) chew(in <-chan Context, contextsCount int) <-chan ActionResult {
	out := make(chan ActionResult, contextsCount)
	flag := make(chan bool, contextsCount)
	for i := 0; i < contextsCount; i++ {
		go func(t *Task, in <-chan Context, out chan<- ActionResult, flag chan<- bool) {
			for c := range in {
				out <- t.a.Act(c)
			}
			flag <- true
		}(t, in, out, flag)
	}
	go func(out chan<- ActionResult, flag <-chan bool) {
		for i := 0; i < contextsCount; i++ {
			<-flag
		}
		close(out)
	}(out, flag)
	return out
}

func (t *Task) digest(out <-chan ActionResult) <-chan TaskResult {
	finished := make(chan TaskResult)
	go func(out <-chan ActionResult, finished chan<- TaskResult) {
		var taskResults TaskResult
		for r := range out {
			taskResults = append(taskResults, r)
		}
		t.logger.Infof("all done with: %+v", taskResults)
		finished <- taskResults
	}(out, finished)
	return finished
}

func (t *Task) Run(m queue.Message) (TaskResult, error) {
	if contexts, err := t.cs.BytesToContexts(m.Payload()); err != nil {
		t.logger.Err(err)
		return nil, err
	} else {
		if contexts != nil && len(contexts) > 0 {
			t.logger.Infof("going to deal with: %+v", contexts)
			return <-t.digest(t.chew(t.feed(contexts), len(contexts))), nil
		} else {
			return TaskResult{}, nil
		}
	}
}

func NewTask(a Action, cs ContextStreamer, logger logging.Logger) *Task {
	return &Task{a: a, cs: cs, logger: logger}
}
