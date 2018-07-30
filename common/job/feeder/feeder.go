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

	"github.com/samwooo/bolsa/common/job/feeder/imp"
	"github.com/samwooo/bolsa/common/job/model"
	"github.com/samwooo/bolsa/common/logging"
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
	logger logging.Logger
	quit   chan bool
	output chan model.Done
	closed atomic.Value
	imp    feederImp
}

func (jf *Feeder) Adapt() chan model.Done { return jf.output }
func (jf *Feeder) Name() string {
	if jf.imp != nil {
		return jf.imp.Name()
	} else {
		return fmt.Sprintf("default")
	}
}
func (jf *Feeder) Retry(d model.Done) {
	if jf.imp != nil && !jf.Closed() {
		if err := jf.imp.DoRetry(jf.output, d); err != nil {
			jf.logger.Warnf("✗ feeder %s retry failed ( %s )", jf.Name(), err.Error())
		} else {
			jf.logger.Debugf("✗ feeder %s retry ( %+v )", jf.Name(), d)
		}
	} else {
		jf.logger.Warnf("✗ feeder %s retry skipped, channel closed ?", jf.Name())
	}
}
func (jf *Feeder) Push(data interface{}) {
	if jf.imp != nil && !jf.Closed() {
		if err := jf.imp.DoPush(jf.output, data); err != nil {
			jf.logger.Warnf("✗ feeder %s push failed ( %s )", jf.Name(), err.Error())
		} else {
			jf.logger.Debugf("✗ feeder %s push ( %+v )", jf.Name(), data)
		}
	} else {
		jf.logger.Warnf("✗ feeder %s push skipped, channel closed ?", jf.Name())
	}
}
func (jf *Feeder) Close() {
	if !jf.Closed() {
		jf.quit <- true
	}
}
func (jf *Feeder) Closed() bool {
	if closed, ok := jf.closed.Load().(bool); ok {
		return closed
	} else {
		jf.logger.Warnf("✗ feeder %s cast closed failed", jf.Name())
		return true
	}
}
func newFeeder(ctx context.Context, logger logging.Logger, RIPRightAfterInit bool, f feederImp) *Feeder {
	initClosed := func() (closed atomic.Value) {
		closed.Store(false)
		return
	}
	jf := Feeder{logger, make(chan bool), make(chan model.Done, runtime.NumCPU()*64),
		initClosed(), f}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGILL, syscall.SIGSYS,
		syscall.SIGTERM, syscall.SIGTRAP, syscall.SIGQUIT, syscall.SIGABRT)
	go func() {
		for {
			select {
			case <-ctx.Done():
				jf.logger.Warnf("⏳ cancellation, feeder %s quiting...", jf.Name())
				jf.Close()
				return
			case s := <-sig:
				jf.logger.Warnf("⏳ signal ( %+v ) feeder %s quiting...", s, jf.Name())
				jf.Close()
				return
			default:
				time.Sleep(time.Millisecond * 10)
			}
		}
	}()

	go func() {
		if jf.imp != nil && !jf.Closed() {
			if err := jf.imp.DoInit(jf.output); err != nil {
				jf.logger.Warnf("✗ feeder %s init failed ( %s )", jf.Name(), err.Error())
			} else {
				jf.logger.Debugf("✗ feeder %s init succeed", jf.Name())
			}
		} else {
			jf.logger.Warnf("✗ feeder %s init skipped, channel closed ?", jf.Name())
		}
		if RIPRightAfterInit {
			jf.Close()
		}
	}()
	go func() {
		for {
			select {
			case <-jf.quit:
				if jf.imp != nil && !jf.Closed() {
					if err := jf.imp.DoExit(jf.output); err != nil {
						jf.logger.Warnf("✗ feeder %s exit failed ( %s )", jf.Name(), err.Error())
					}
				} else {
					jf.logger.Warnf("✗ feeder %s exit skipped, channel closed ?", jf.Name())
				}
				close(jf.output)
				jf.closed.Store(true)
				return
			default:
				if jf.imp != nil && !jf.Closed() {
					if err := jf.imp.DoWork(jf.output); err != nil {
						jf.logger.Warnf("✗ feeder %s work failed ( %s )", jf.Name(), err.Error())
					}
				} else {
					jf.logger.Warnf("✗ feeder %s work skipped, channel closed ?", jf.Name())
					time.Sleep(time.Millisecond * 5)
				}
			}
		}
	}()

	return &jf
}

func NewChanFeeder(ctx context.Context, logger logging.Logger, labor model.Labor) *Feeder {
	return newFeeder(ctx, logger, false, imp.NewChanFeederImp(ctx, labor))
}

func NewDataFeeder(ctx context.Context, logger logging.Logger, data []interface{}, batch int,
	RIPRightAfterInit bool) *Feeder {
	return newFeeder(ctx, logger, RIPRightAfterInit, imp.NewDataFeederImp(data, batch))
}
