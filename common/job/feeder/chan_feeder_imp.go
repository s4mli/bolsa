package feeder

import (
	"context"
	"fmt"

	"github.com/samwooo/bolsa/common/job/share"
)

//////////////////////
// Chan Feeder IMP //
type chanFeederImp struct {
	ctx   context.Context
	labor share.Labor
}

func (cf *chanFeederImp) name() string                    { return fmt.Sprintf("chan") }
func (cf *chanFeederImp) doInit(ch chan share.Done) error { return nil }
func (cf *chanFeederImp) doExit(ch chan share.Done) error { return nil }
func (cf *chanFeederImp) doWork(ch chan share.Done) error {
	if cf.labor == nil {
		return fmt.Errorf("✗ missing loabor")
	} else {
		if r, err := cf.labor(cf.ctx, nil); err != nil {
			return err
		} else {
			ch <- share.NewDone(nil, r, nil, 0, r, share.KeyFrom(r))
			return nil
		}
	}
}
func (cf *chanFeederImp) doRetry(ch chan share.Done, d share.Done) error {
	ch <- d
	return nil
}
func (cf *chanFeederImp) doPush(ch chan share.Done, data interface{}) error {
	store := func(d interface{}) {
		ch <- share.NewDone(nil, d, nil, 0, d, share.KeyFrom(d))
	}
	if dataArray, ok := data.([]interface{}); ok {
		for _, d := range dataArray {
			store(d)
		}
	} else {
		store(data)
	}
	return nil
}
func newChanFeederImp(ctx context.Context, labor share.Labor) feederImp {
	return &chanFeederImp{ctx, labor}
}
