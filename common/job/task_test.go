package job

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/common/logging"
	"github.com/stretchr/testify/assert"
)

func TestTaskWithSingleWorkerWithoutBatch(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", common.APP_NAME),
		logging.LogLevelFromString("INFO"), 100)

	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9}
	output := NewTask(logging.GetLogger(" task test "), "TestTaskWithoutBatch",
		func(ctx context.Context, d Done) Done {
			return newDone(nil, d.P, nil)
		}).Run(context.Background(), 1, 1, NewDataFeeder(with).Adapt())

	for d := range output {
		assert.Equal(t, true, common.IsIn(d.R, with))
	}
}

func TestTaskWithNWorkersWithoutBatch(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", common.APP_NAME),
		logging.LogLevelFromString("INFO"), 100)

	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9}
	output := NewTask(logging.GetLogger(" task test "), "TestTaskWithoutBatch",
		func(ctx context.Context, d Done) Done {
			return newDone(nil, d.P, nil)
		}).Run(context.Background(), runtime.NumCPU(), 1, NewDataFeeder(with).Adapt())

	for d := range output {
		assert.Equal(t, true, common.IsIn(d.R, with))
	}
}

func TestTaskWithSingleWorkerWithBatch(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", common.APP_NAME),
		logging.LogLevelFromString("INFO"), 100)

	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9}
	output := NewTask(logging.GetLogger(" task test "), "TestTaskWithoutBatch",
		func(ctx context.Context, d Done) Done {
			return newDone(nil, d.P, nil)
		}).Run(context.Background(), 1, 2, NewDataFeeder(with).Adapt())

	for d := range output {
		rs, ok := d.R.([]interface{})
		assert.Equal(t, true, ok)
		for _, r := range rs {
			assert.Equal(t, true, common.IsIn(r, with))
		}
	}
}

func TestTaskWithNWorkersWithBatch(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", common.APP_NAME),
		logging.LogLevelFromString("INFO"), 100)

	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9}
	output := NewTask(logging.GetLogger(" task test "), "TestTaskWithoutBatch",
		func(ctx context.Context, d Done) Done {
			return newDone(nil, d.P, nil)
		}).Run(context.Background(), runtime.NumCPU(), 3, NewDataFeeder(with).Adapt())

	for d := range output {
		rs, ok := d.R.([]interface{})
		assert.Equal(t, true, ok)
		for _, r := range rs {
			assert.Equal(t, true, common.IsIn(r, with))
		}
	}
}
