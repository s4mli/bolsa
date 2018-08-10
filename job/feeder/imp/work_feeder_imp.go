package imp

import (
	"github.com/samwooo/bolsa/job/model"
)

////////////////////////////////////////////////////////////////////////////////////
// 1: If only one single data would appear in Work, then simply call Labor(date) //
// 2: If have to iterate multiple data then call Labor(data) on each single one //
type Work func(model.Labor) error
type Init func(chan model.Done) error
type Exit func(chan model.Done) error

//////////////////////
// Work Feeder IMP //
type workFeederImp struct {
	init  Init
	work  Work
	labor model.Labor
	exit  Exit
}

func (wf *workFeederImp) Name() string { return "work" }
func (wf *workFeederImp) DoInit(ch chan model.Done) error {
	if wf.init != nil {
		return wf.init(ch)
	} else {
		return nil
	}
}
func (wf *workFeederImp) DoExit(ch chan model.Done) error {
	if wf.exit != nil {
		return wf.exit(ch)
	} else {
		return nil
	}
}
func (wf *workFeederImp) DoWork(ch chan model.Done) error {
	labor := func() model.Labor {
		if wf.labor != nil {
			return func(p interface{}) (interface{}, error) {
				r, err := wf.labor(p)
				if err == nil {
					ch <- model.NewDone(nil, r, nil, 0, r, model.KeyFrom(r))
				}
				return r, err
			}
		} else {
			return func(p interface{}) (interface{}, error) {
				ch <- model.NewDone(nil, p, nil, 0, p, model.KeyFrom(p))
				return p, nil
			}
		}
	}
	if wf.work != nil {
		return wf.work(labor())
	} else {
		return nil
	}
}
func (wf *workFeederImp) DoRetry(ch chan model.Done, d model.Done) error {
	ch <- d
	return nil
}
func (wf *workFeederImp) DoPush(ch chan model.Done, data interface{}) error {
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
func NewWorkFeederImp(init Init, work Work, labor model.Labor, exit Exit) *workFeederImp {
	return &workFeederImp{init, work, labor, exit}
}
