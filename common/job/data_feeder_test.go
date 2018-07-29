package job

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/common/logging"
	"github.com/stretchr/testify/assert"
)

func testWithInSingleGoroutine(t *testing.T, noDrama, usingContext bool, batch int) {
	data := []interface{}{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	var ctx = context.Background()
	var cancelFn context.CancelFunc
	if usingContext {
		ctx, cancelFn = context.WithDeadline(context.Background(), time.Now().Add(
			time.Duration(len(data)*20)*time.Millisecond))
		defer cancelFn()
	}
	f := NewDataFeeder(ctx, logging.GetLogger(""), data, batch, noDrama)
	if !usingContext {
		time.AfterFunc(time.Duration(len(data)*20)*time.Millisecond, func() { f.Close() })
	}
	done := make(chan bool)
	count := 0
	go func() {
		for d := range f.Adapt() {
			count++
			if batch <= 0 {
				assert.Equal(t, data, d.R)
			} else if batch == 1 {
				assert.Equal(t, true, common.IsIn(d.R, data))
			} else {
				rs, ok := d.R.([]interface{})
				assert.Equal(t, true, ok)
				for _, r := range rs {
					assert.Equal(t, true, common.IsIn(r, data))
				}
			}
		}
		done <- true
	}()
	<-done
	group := func() int {
		if batch <= 0 {
			return 1
		} else {
			group := len(data) / batch
			if len(data)%batch > 0 {
				group += 1
			}
			return group
		}
	}
	assert.Equal(t, group(), count)
}

func testWithinMultipleGoroutines(t *testing.T, noDrama, usingContext bool, batch int) {
	data := []interface{}{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	var ctx = context.Background()
	var cancelFn context.CancelFunc
	if usingContext {
		ctx, cancelFn = context.WithDeadline(context.Background(), time.Now().Add(
			time.Duration(len(data)*20)*time.Millisecond))
		defer cancelFn()
	}
	f := NewDataFeeder(ctx, logging.GetLogger(""), data, batch, noDrama)
	if !usingContext {
		time.AfterFunc(time.Duration(len(data)*20)*time.Millisecond, func() { f.Close() })
	}
	workers := runtime.NumCPU()
	done := make(chan bool, workers)
	out := make(chan interface{}, workers)
	count := 0
	for i := 0; i < workers; i++ {
		go func() {
			for d := range f.Adapt() {
				out <- d.R
			}
			done <- true
		}()
	}
	go func() {
		for i := 0; i < workers; i++ {
			<-done
		}
		close(out)
	}()
	for d := range out {
		count++
		if batch <= 0 {
			assert.Equal(t, data, d)
		} else if batch == 1 {
			assert.Equal(t, true, common.IsIn(d, data))
		} else {
			rs, ok := d.([]interface{})
			assert.Equal(t, true, ok)
			for _, r := range rs {
				assert.Equal(t, true, common.IsIn(r, data))
			}
		}
	}
	group := func() int {
		if batch <= 0 {
			return 1
		} else {
			group := len(data) / batch
			if len(data)%batch > 0 {
				group += 1
			}
			return group
		}
	}
	assert.Equal(t, group(), count)
}

func TestDataFeederWithinSingleGoroutineWithContext(t *testing.T) {
	testWithInSingleGoroutine(t, true, true, -1)
	testWithInSingleGoroutine(t, true, true, 0)
	testWithInSingleGoroutine(t, true, true, 1)
	testWithInSingleGoroutine(t, true, true, 2)
	testWithInSingleGoroutine(t, true, true, 12)
	testWithInSingleGoroutine(t, false, true, -1)
	testWithInSingleGoroutine(t, false, true, 0)
	testWithInSingleGoroutine(t, false, true, 1)
	testWithInSingleGoroutine(t, false, true, 2)
	testWithInSingleGoroutine(t, false, true, 12)
}

func TestDataFeederWithinSingleGoroutineWithDeadline(t *testing.T) {
	testWithInSingleGoroutine(t, true, false, -1)
	testWithInSingleGoroutine(t, true, false, 0)
	testWithInSingleGoroutine(t, true, false, 1)
	testWithInSingleGoroutine(t, true, false, 2)
	testWithInSingleGoroutine(t, true, false, 12)
	testWithInSingleGoroutine(t, false, false, -1)
	testWithInSingleGoroutine(t, false, false, 0)
	testWithInSingleGoroutine(t, false, false, 1)
	testWithInSingleGoroutine(t, false, false, 2)
	testWithInSingleGoroutine(t, false, false, 12)
}

func TestDataFeederWithinMultipleGoroutinesWithContext(t *testing.T) {
	testWithinMultipleGoroutines(t, true, true, -1)
	testWithinMultipleGoroutines(t, true, true, 0)
	testWithinMultipleGoroutines(t, true, true, 1)
	testWithinMultipleGoroutines(t, true, true, 2)
	testWithinMultipleGoroutines(t, true, true, 12)
	testWithinMultipleGoroutines(t, false, true, -1)
	testWithinMultipleGoroutines(t, false, true, 0)
	testWithinMultipleGoroutines(t, false, true, 1)
	testWithinMultipleGoroutines(t, false, true, 2)
	testWithinMultipleGoroutines(t, false, true, 12)
}

func TestDataFeederWithinMultipleGoroutinesWithDeadline(t *testing.T) {
	testWithinMultipleGoroutines(t, true, false, -1)
	testWithinMultipleGoroutines(t, true, false, 0)
	testWithinMultipleGoroutines(t, true, false, 1)
	testWithinMultipleGoroutines(t, true, false, 2)
	testWithinMultipleGoroutines(t, true, false, 12)
	testWithinMultipleGoroutines(t, false, false, -1)
	testWithinMultipleGoroutines(t, false, false, 0)
	testWithinMultipleGoroutines(t, false, false, 1)
	testWithinMultipleGoroutines(t, false, false, 2)
	testWithinMultipleGoroutines(t, false, false, 12)
}
