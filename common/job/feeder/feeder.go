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

	"github.com/samwooo/bolsa/common/job/share"
	"github.com/samwooo/bolsa/common/logging"
)

/////////////////
// Feeder IMP //
type feederImp interface {
	name() string
	doPush(chan share.Done, interface{}) error // push sth into a feeder at anytime
	doInit(chan share.Done) error              // do sth when init
	doWork(chan share.Done) error              // do things
	doExit(chan share.Done) error              // do sth when exit
	doRetry(chan share.Done, share.Done) error // do retry
}

/////////////////
// Job Feeder //
type Feeder struct {
	logger logging.Logger
	quit   chan bool
	output chan share.Done
	closed atomic.Value
	feederImp
}

func (jf *Feeder) Adapt() chan share.Done { return jf.output }
func (jf *Feeder) Name() string {
	if jf.feederImp != nil {
		return jf.feederImp.name()
	} else {
		return fmt.Sprintf("default")
	}
}
func (jf *Feeder) Retry(d share.Done) {
	if jf.feederImp != nil && !jf.Closed() {
		if err := jf.feederImp.doRetry(jf.output, d); err != nil {
			jf.logger.Warnf("✗ feeder %s retry failed ( %s )", jf.Name(), err.Error())
		} else {
			jf.logger.Debugf("✗ feeder %s retry ( %+v )", jf.Name(), d)
		}
	} else {
		jf.logger.Warnf("✗ feeder %s retry skipped, channel closed ?", jf.Name())
	}
}
func (jf *Feeder) Push(data interface{}) {
	if jf.feederImp != nil && !jf.Closed() {
		if err := jf.feederImp.doPush(jf.output, data); err != nil {
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
	jf := Feeder{logger, make(chan bool), make(chan share.Done, runtime.NumCPU()*64),
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
		if jf.feederImp != nil && !jf.Closed() {
			if err := jf.feederImp.doInit(jf.output); err != nil {
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
				if jf.feederImp != nil && !jf.Closed() {
					if err := jf.feederImp.doExit(jf.output); err != nil {
						jf.logger.Warnf("✗ feeder %s exit failed ( %s )", jf.Name(), err.Error())
					}
				} else {
					jf.logger.Warnf("✗ feeder %s exit skipped, channel closed ?", jf.Name())
				}
				close(jf.output)
				jf.closed.Store(true)
				return
			default:
				if jf.feederImp != nil && !jf.Closed() {
					if err := jf.feederImp.doWork(jf.output); err != nil {
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

func NewChanFeeder(ctx context.Context, logger logging.Logger, strategy share.LaborStrategy) *Feeder {
	return newFeeder(ctx, logger, false, newChanFeederImp(ctx, strategy))
}

func NewDataFeeder(ctx context.Context, logger logging.Logger, data []interface{}, batch int,
	RIPRightAfterInit bool) *Feeder {
	return newFeeder(ctx, logger, RIPRightAfterInit, newDataFeederImp(ctx, data, batch, RIPRightAfterInit))
}
