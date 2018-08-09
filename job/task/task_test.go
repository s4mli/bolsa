package task

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/job/feeder"
	"github.com/samwooo/bolsa/job/model"
	"github.com/samwooo/bolsa/logging"
	"github.com/stretchr/testify/assert"
)

var _ = logging.DefaultLogger("", logging.LogLevelFromString("ERROR"), 100)

func testWithNWorker(t *testing.T, workers int, noDrama, usingContext bool) {
	data := []interface{}{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	var ctx = context.Background()
	var cancelFn context.CancelFunc = nil
	if usingContext {
		ctx, cancelFn = context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*200))
		defer cancelFn()
	}
	f := feeder.NewDataFeeder(ctx, logging.GetLogger(""), runtime.NumCPU(), data, 1, noDrama)
	if !usingContext {
		time.AfterFunc(time.Millisecond*200, func() { f.Close() })
	}
	output := NewTask(logging.GetLogger(""), "",
		func(d model.Done) (model.Done, bool) {
			return model.NewDone(nil, d.P, nil, 0, d.D, d.Key), true
		}).Run(workers, f.Adapt())

	for d := range output {
		assert.Equal(t, true, common.IsIn(d.R, data))
	}
}

func TestTaskWithSingleWorkerWithContext(t *testing.T) {
	testWithNWorker(t, 1, true, true)
	testWithNWorker(t, 1, false, true)
}

func TestTaskWithSingleWorkerWithDeadline(t *testing.T) {
	testWithNWorker(t, 1, true, false)
	testWithNWorker(t, 1, false, false)
}

func TestTaskWithNWorkersWithContext(t *testing.T) {
	testWithNWorker(t, runtime.NumCPU(), true, true)
	testWithNWorker(t, runtime.NumCPU(), false, true)
}

func TestTaskWithNWorkersWithDeadline(t *testing.T) {
	testWithNWorker(t, runtime.NumCPU(), true, false)
	testWithNWorker(t, runtime.NumCPU(), false, false)
}
