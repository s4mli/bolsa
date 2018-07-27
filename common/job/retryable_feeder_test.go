package job

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/samwooo/bolsa/common"
	"github.com/stretchr/testify/assert"
)

func testWithInSingleGoroutine(t *testing.T, noDrama, usingContext bool) {
	data := []interface{}{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	var ctx = context.Background()
	var cancelFn context.CancelFunc
	if usingContext {
		ctx, cancelFn = context.WithDeadline(context.Background(), time.Now().Add(
			time.Duration(len(data)*20)*time.Millisecond))
		defer cancelFn()
	}
	f := NewRetryableFeeder(ctx, data, noDrama)
	if !usingContext {
		time.AfterFunc(time.Duration(len(data)*20)*time.Millisecond, func() { f.Close() })
	}
	done := make(chan bool)
	count := 0
	go func() {
		for d := range f.Adapt() {
			count++
			assert.Equal(t, true, common.IsIn(d.R, data))
		}
		done <- true
	}()
	<-done
	assert.Equal(t, len(data), count)
}

func testWithinMultipleGoroutines(t *testing.T, noDrama, usingContext bool) {
	data := []interface{}{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	var ctx = context.Background()
	var cancelFn context.CancelFunc
	if usingContext {
		ctx, cancelFn = context.WithDeadline(context.Background(), time.Now().Add(
			time.Duration(len(data)*20)*time.Millisecond))
		defer cancelFn()
	}
	f := NewRetryableFeeder(ctx, data, noDrama)
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
				assert.Equal(t, true, common.IsIn(d.R, data))
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
		assert.Equal(t, true, common.IsIn(d, data))
	}
	assert.Equal(t, len(data), count)
}

func TestRetryableFeederWithinSingleGoroutineWithContext(t *testing.T) {
	testWithInSingleGoroutine(t, true, true)
	testWithInSingleGoroutine(t, false, true)
}

func TestRetryableFeederWithinSingleGoroutineWithDeadline(t *testing.T) {
	testWithInSingleGoroutine(t, true, false)
	testWithInSingleGoroutine(t, false, false)
}

func TestRetryableFeederWithinMultipleGoroutinesWithContext(t *testing.T) {
	testWithinMultipleGoroutines(t, true, true)
	testWithinMultipleGoroutines(t, false, true)
}

func TestRetryableFeederWithinMultipleGoroutinesWithDeadline(t *testing.T) {
	testWithinMultipleGoroutines(t, true, false)
	testWithinMultipleGoroutines(t, false, false)
}
