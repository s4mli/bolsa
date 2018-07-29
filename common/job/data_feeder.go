package job

import (
	"context"
	"fmt"
	"sync"

	"strings"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/common/logging"
)

//////////////////////
// Data Feeder IMP //
type dataFeeder struct {
	initial []interface{}
	shift   sync.Map
	batch   int
	batchStrategy
}

func (df *dataFeeder) pump(ch chan Done) error {
	var errs []string
	df.shift.Range(func(key, value interface{}) bool {
		if d, ok := value.(Done); ok {
			ch <- d
		} else {
			errs = append(errs, fmt.Sprintf("✗ cast error ( %+v => %+v )", key, value))
		}
		df.shift.Delete(key)
		return true
	})
	return common.ErrorFromString(strings.Join(errs, " | "))
}
func (df *dataFeeder) name() string              { return fmt.Sprintf("data") }
func (df *dataFeeder) doInit(ch chan Done) error { return df.doPush(ch, df.initial) }
func (df *dataFeeder) doWork(ch chan Done) error { return df.pump(ch) }
func (df *dataFeeder) doExit(ch chan Done) error { return df.pump(ch) }
func (df *dataFeeder) doRetry(ch chan Done, d Done) error {
	df.shift.Store(d.String(), d)
	return nil
}
func (df *dataFeeder) doPush(ch chan Done, data interface{}) error {
	store := func(d interface{}) {
		done := NewDone(nil, d, nil, 0, d, KeyFrom(d))
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
func (df *dataFeeder) Batch() int { return df.batch }
func NewDataFeeder(ctx context.Context, logger logging.Logger, data []interface{}, batch int,
	RIPRightAfterInit bool) *Feeder {

	df := dataFeeder{data, sync.Map{}, batch, nil}
	df.batchStrategy = &df
	return newFeeder(ctx, logger, RIPRightAfterInit, &df)
}
