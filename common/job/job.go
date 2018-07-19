package job

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/samwooo/bolsa/common/logging"
)

func (j *Job) BatchStrategy(bh batchStrategy) *Job {
	j.batchStrategy = bh
	return j
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

func (j *Job) Run(ctx context.Context, f feeder) []Done {
	sig, finished, quit := make(chan os.Signal), make(chan bool), make(chan bool)
	signals := []os.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGILL, syscall.SIGSYS, syscall.SIGSTOP,
		syscall.SIGKILL, syscall.SIGTERM, syscall.SIGTRAP, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGSTKFLT}
	signal.Notify(sig, signals...)

	runJob := func() []Done {
		jobDone := j.run(ctx, f)
		if j.retryStrategy != nil {
			jobDone = j.retry(ctx, jobDone, quit)
		}
		finished <- true
		j.Logger.Infof("♥ %s finished.", j.name)
		return jobDone
	}

	exitUntilFinished := func(reason string) {
		quit <- true
		j.Logger.Info(reason)
		<-finished
	}

	go func() {
		for {
			select {
			case s := <-sig:
				exitUntilFinished(fmt.Sprintf("⏳ signal ( %+v ), %s quiting...", s, j.name))
				return
			case <-ctx.Done():
				exitUntilFinished(fmt.Sprintf("⏳ cancellation, %s quiting...", j.name))
				return
			case <-finished:
				return
			}
		}
	}()

	result := runJob()
	return result
}

func NewJob(logger logging.Logger, name string, workers int) *Job {
	if workers <= 0 {
		workers = runtime.NumCPU() * 64
	}
	j := &Job{logger, name, workers,
		nil, nil,
		nil, nil}
	return j
}
