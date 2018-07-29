package feeder

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/common/job/share"
)

//////////////////////
// Data Feeder IMP //
type dataFeederImp struct {
	initial []interface{}
	shift   sync.Map
	batch   int
	share.BatchStrategy
}

func (df *dataFeederImp) pump(ch chan share.Done) error {
	var errs []string
	df.shift.Range(func(key, value interface{}) bool {
		if d, ok := value.(share.Done); ok {
			ch <- d
		} else {
			errs = append(errs, fmt.Sprintf("✗ cast error ( %+v => %+v )", key, value))
		}
		df.shift.Delete(key)
		return true
	})
	return common.ErrorFromString(strings.Join(errs, " | "))
}
func (df *dataFeederImp) name() string                    { return fmt.Sprintf("data") }
func (df *dataFeederImp) doInit(ch chan share.Done) error { return df.doPush(ch, df.initial) }
func (df *dataFeederImp) doWork(ch chan share.Done) error { return df.pump(ch) }
func (df *dataFeederImp) doExit(ch chan share.Done) error { return df.pump(ch) }
func (df *dataFeederImp) doRetry(ch chan share.Done, d share.Done) error {
	df.shift.Store(d.String(), d)
	return nil
}
func (df *dataFeederImp) doPush(ch chan share.Done, data interface{}) error {
	store := func(d interface{}) {
		done := share.NewDone(nil, d, nil, 0, d, share.KeyFrom(d))
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
func (df *dataFeederImp) Batch() int { return df.batch }
func newDataFeederImp(ctx context.Context, data []interface{}, batch int, RIPRightAfterInit bool) feederImp {
	df := dataFeederImp{data, sync.Map{}, batch, nil}
	df.BatchStrategy = &df
	return &df
}
