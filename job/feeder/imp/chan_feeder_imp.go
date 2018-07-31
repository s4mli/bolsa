package imp

import (
	"fmt"

	"github.com/samwooo/bolsa/job/model"
)

//////////////////////
// Chan Feeder IMP //
type chanFeederImp struct{ labor model.Labor }

func (cf *chanFeederImp) Name() string                    { return fmt.Sprintf("chan") }
func (cf *chanFeederImp) DoInit(ch chan model.Done) error { return nil }
func (cf *chanFeederImp) DoExit(ch chan model.Done) error { return nil }
func (cf *chanFeederImp) DoWork(ch chan model.Done) error {
	if cf.labor == nil {
		return fmt.Errorf("✗ missing loabor")
	} else {
		if r, err := cf.labor(nil); err != nil {
			return err
		} else {
			ch <- model.NewDone(nil, r, nil, 0, r, model.KeyFrom(r))
			return nil
		}
	}
}
func (cf *chanFeederImp) DoRetry(ch chan model.Done, d model.Done) error {
	ch <- d
	return nil
}
func (cf *chanFeederImp) DoPush(ch chan model.Done, data interface{}) error {
	store := func(d interface{}) {
		ch <- model.NewDone(nil, d, nil, 0, d, model.KeyFrom(d))
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
func NewChanFeederImp(labor model.Labor) *chanFeederImp {
	return &chanFeederImp{labor}
}
