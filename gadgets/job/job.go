package job

import (
	"time"

	"github.com/samwooo/bolsa/gadgets/logging"
	"github.com/samwooo/bolsa/gadgets/queue"
	"github.com/samwooo/bolsa/gadgets/store"
	"github.com/samwooo/bolsa/gadgets/util"
)

type Job struct {
	TaskRescue
	t           *Task
	q           queue.Queue
	s           store.Store
	logger      logging.Logger
	workerCount int
	maxSleep    int
}

func (j *Job) feed() <-chan queue.Message {
	in := make(chan queue.Message, j.workerCount*16)
	for i := 0; i < j.workerCount; i++ {
		go func(j *Job, i int, in chan<- queue.Message) {
			for {
				if queueMsg, err := j.q.ReceiveMessage(); err != nil {
					time.Sleep(util.RandomDuration(j.maxSleep))
				} else {
					j.logger.Debugf("got message %+v", queueMsg)
					in <- queueMsg
				}
			}
		}(j, i, in)
	}
	return in
}

func (j *Job) chew(in <-chan queue.Message) <-chan TaskResult {
	out := make(chan TaskResult, j.workerCount*16)
	for i := 0; i < j.workerCount*2; i++ {
		go func(j *Job, i int, in <-chan queue.Message, out chan<- TaskResult) {
			for {
				message := <-in
				if results, err := j.t.Run(message); err != nil {
					j.logger.Warn("Unable to unmarshal message, will drop it")
					time.Sleep(util.RandomDuration(j.maxSleep))
				} else {
					j.logger.Debugf("finished with %+v", results)
					out <- results
				}
				message.Delete()
			}
		}(j, i, in, out)
	}
	return out
}

func (j *Job) digest(out <-chan TaskResult) {
	for i := 0; i < j.workerCount*2; i++ {
		go func(j *Job, i int, out <-chan TaskResult) {
			for {
				j.Rescue(<-out)
				time.Sleep(time.Duration(time.Second))
			}
		}(j, i, out)
	}
}

func (j *Job) Start() {
	j.digest(j.chew(j.feed()))
}

func NewJob(a Action, cs ContextStreamer, tr TaskRescue, q queue.Queue, s store.Store,
	workerCount, maxSleep int, logger logging.Logger) *Job {
	job := &Job{
		t:           NewTask(a, cs, logger),
		q:           q,
		s:           s,
		logger:      logger,
		workerCount: workerCount,
		maxSleep:    maxSleep}
	job.TaskRescue = tr
	return job
}
