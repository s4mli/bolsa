package feeder

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/samwooo/bolsa/job/feeder/imp"
	"github.com/samwooo/bolsa/job/model"
	"github.com/samwooo/bolsa/logging"
)

/////////////////
// Feeder IMP //
type feederImp interface {
	Name() string
	DoPush(chan model.Done, interface{}) error // push sth into a feeder at anytime
	DoInit(chan model.Done) error              // do sth when init
	DoWork(chan model.Done) error              // do things
	DoExit(chan model.Done) error              // do sth when exit
	DoRetry(chan model.Done, model.Done) error // do retry
}

/////////////////
// Job Feeder //
type Feeder struct {
	logger  logging.Logger
	workers int
	output  chan model.Done
	closed  atomic.Value
	feederImp
}

func (jf *Feeder) Adapt() chan model.Done { return jf.output }
func (jf *Feeder) Name() string {
	if jf.feederImp != nil {
		return jf.feederImp.Name()
	} else {
		return fmt.Sprintf("default")
	}
}
func (jf *Feeder) Retry(d model.Done) {
	if jf.feederImp != nil && !jf.Closed() {
		if err := jf.feederImp.DoRetry(jf.output, d); err != nil {
			jf.logger.Warnf("✗ feeder %s retry failed ( %s )", jf.Name(), err.Error())
		} else {
			jf.logger.Debugf("✔ feeder %s retry ( %+v )", jf.Name(), d)
		}
	} else {
		jf.logger.Debugf("✗ feeder %s retry skipped", jf.Name())
	}
}
func (jf *Feeder) Push(data interface{}) error {
	if jf.feederImp != nil && !jf.Closed() {
		if err := jf.feederImp.DoPush(jf.output, data); err != nil {
			jf.logger.Warnf("✗ feeder %s push failed ( %s )", jf.Name(), err.Error())
			return err
		} else {
			jf.logger.Debugf("✔ feeder %s push ( %+v )", jf.Name(), data)
			return nil
		}
	} else {
		err := fmt.Errorf("✗ feeder %s push ( %+v ) skipped", jf.Name(), data)
		jf.logger.Info(err)
		return err
	}
}
func (jf *Feeder) Close() { jf.closed.Store(true) }

func (jf *Feeder) Closed() bool {
	if closed, ok := jf.closed.Load().(bool); ok {
		return closed
	} else {
		jf.logger.Warnf("✗ feeder %s cast closed failed", jf.Name())
		return true
	}
}
func newFeeder(ctx context.Context, logger logging.Logger, workers int, RIPRightAfterInit bool, f feederImp) *Feeder {
	initClosed := func() (closed atomic.Value) {
		closed.Store(false)
		return
	}
	if workers <= 0 {
		workers = runtime.NumCPU() * 64
	}
	jf := Feeder{logger, workers,
		make(chan model.Done, workers),
		initClosed(), f}
	sig, waitress := make(chan os.Signal, 1), make(chan bool, workers)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGILL, syscall.SIGSYS,
		syscall.SIGTERM, syscall.SIGTRAP, syscall.SIGQUIT, syscall.SIGABRT)
	go func() {
		for {
			select {
			case <-ctx.Done():
				jf.logger.Errorf("⏳ cancellation, feeder %s quiting...", jf.Name())
				jf.Close()
				return
			case s := <-sig:
				jf.logger.Errorf("⏳ signal ( %+v ) feeder %s quiting...", s, jf.Name())
				jf.Close()
				return
			default:
				time.Sleep(time.Millisecond * 10)
			}
		}
	}()

	go func() {
		if jf.feederImp != nil && !jf.Closed() {
			if err := jf.feederImp.DoInit(jf.output); err != nil {
				jf.logger.Warnf("✗ feeder %s init failed ( %s )", jf.Name(), err.Error())
			} else {
				jf.logger.Debugf("✗ feeder %s init succeed", jf.Name())
			}
		} else {
			jf.logger.Debugf("✗ feeder %s init skipped", jf.Name())
		}
		if RIPRightAfterInit {
			jf.Close()
		}
	}()

	go func() {
		for i := 0; i < jf.workers; i++ {
			<-waitress
		}
		close(jf.output)
	}()

	for i := 0; i < jf.workers; i++ {
		go func() {
			for {
				if jf.Closed() {
					if jf.feederImp != nil {
						if err := jf.feederImp.DoExit(jf.output); err != nil {
							jf.logger.Warnf("✗ feeder %s exit failed ( %s )", jf.Name(), err.Error())
						}
					}
					waitress <- true
					return
				} else {
					if jf.feederImp != nil {
						if err := jf.feederImp.DoWork(jf.output); err != nil {
							jf.logger.Warnf("✗ feeder %s work failed ( %s )", jf.Name(), err.Error())
						}
					} else {
						jf.logger.Debugf("✗ feeder %s work skipped", jf.Name())
						time.Sleep(time.Millisecond * 5)
					}
				}
			}
		}()
	}

	return &jf
}

func NewWorkFeeder(ctx context.Context, logger logging.Logger, workers int, init imp.Init, work imp.Work,
	labor model.Labor, exit imp.Exit) *Feeder {
	return newFeeder(ctx, logger, workers, false, imp.NewWorkFeederImp(init, work, labor, exit))
}

func NewDataFeeder(ctx context.Context, logger logging.Logger, workers int, data []interface{}, batch int,
	RIPRightAfterInit bool) *Feeder {
	return newFeeder(ctx, logger, workers, RIPRightAfterInit, imp.NewDataFeederImp(data, batch))
}
