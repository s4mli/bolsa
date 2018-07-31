package imp

import (
	"fmt"
	"strings"
	"sync"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/job/model"
)

//////////////////////
// Data Feeder IMP //
type dataFeederImp struct {
	initial []interface{}
	shift   sync.Map
	batch   int
}

func (df *dataFeederImp) pump(ch chan model.Done) error {
	var errs []string
	df.shift.Range(func(key, value interface{}) bool {
		if d, ok := value.(model.Done); ok {
			ch <- d
		} else {
			errs = append(errs, fmt.Sprintf("✗ cast error ( %+v => %+v )", key, value))
		}
		df.shift.Delete(key)
		return true
	})
	return common.ErrorFromString(strings.Join(errs, " | "))
}
func (df *dataFeederImp) Name() string                    { return fmt.Sprintf("data") }
func (df *dataFeederImp) DoInit(ch chan model.Done) error { return df.DoPush(ch, df.initial) }
func (df *dataFeederImp) DoWork(ch chan model.Done) error { return df.pump(ch) }
func (df *dataFeederImp) DoExit(ch chan model.Done) error { return df.pump(ch) }
func (df *dataFeederImp) DoRetry(ch chan model.Done, d model.Done) error {
	df.shift.Store(d.String(), d)
	return nil
}
func (df *dataFeederImp) DoPush(ch chan model.Done, data interface{}) error {
	store := func(d interface{}) {
		done := model.NewDone(nil, d, nil, 0, d, model.KeyFrom(d))
		df.shift.Store(done.String(), done)
	}
	// no batch
	if df.batch <= 0 {
		store(data)
	} else {
		// do batch and data is an array
		if dataArray, ok := data.([]interface{}); ok {
			count := len(dataArray)
			group := count / df.batch
			if count%df.batch > 0 {
				group += 1
			}
			for i := 0; i < group; i++ {
				start := i * df.batch
				end := i*df.batch + df.batch
				if end > count {
					end = count
				}
				batchedData := dataArray[start:end]
				// batch <= 1
				if df.batch <= 1 {
					store(batchedData[0])
				} else {
					store(batchedData)
				}
			}
		} else {
			// data is not an array
			store(data)
		}
	}
	return nil
}
func NewDataFeederImp(data []interface{}, batch int) *dataFeederImp {
	df := dataFeederImp{data, sync.Map{}, batch}
	return &df
}
