package job

import (
	"context"
	"fmt"

	"github.com/samwooo/bolsa/common/logging"
)

//////////////////
// Chan Feeder //
type chanFeeder struct {
	ctx context.Context
	laborStrategy
}

func (cf *chanFeeder) name() string              { return fmt.Sprintf("chan") }
func (cf *chanFeeder) doInit(ch chan Done) error { return nil }
func (cf *chanFeeder) doExit(ch chan Done) error { return nil }
func (cf *chanFeeder) doWork(ch chan Done) error {
	if r, err := cf.Work(cf.ctx, nil); err != nil {
		return err
	} else {
		ch <- NewDone(nil, r, nil, 0, r, KeyFrom(r))
		return nil
	}
}
func (cf *chanFeeder) doRetry(ch chan Done, d Done) error {
	ch <- d
	return nil
}
func (cf *chanFeeder) doPush(ch chan Done, data interface{}) error {
	store := func(d interface{}) {
		ch <- NewDone(nil, d, nil, 0, d, KeyFrom(d))
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
func (cf *chanFeeder) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	if cf.laborStrategy != nil {
		return cf.laborStrategy.Work(ctx, p)
	} else {
		return p, nil
	}
}
func NewChanFeeder(ctx context.Context, logger logging.Logger, strategy laborStrategy) *Feeder {
	return newFeeder(ctx, logger, false, &chanFeeder{ctx, strategy})
}
