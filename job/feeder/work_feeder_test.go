package feeder

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samwooo/bolsa/job/model"
	"github.com/samwooo/bolsa/logging"
	"github.com/stretchr/testify/assert"
)

var _ = logging.DefaultLogger("", logging.LogLevelFromString("ERROR"), 100)

var COUNTER uint64 = 0

func work(labor model.Labor) error {
	if _, err := labor(nil); err != nil {
		return err
	} else {
		return nil
	}
}

func withoutError(p interface{}) (r interface{}, e error) {
	return atomic.AddUint64(&COUNTER, 2), nil
}

func withError(p interface{}) (r interface{}, e error) {
	return nil, fmt.Errorf("laborError")
}

func testWithSingleGoroutineWithoutError(t *testing.T, usingContext bool) {
	var ctx = context.Background()
	var cancelFn context.CancelFunc
	if usingContext {
		ctx, cancelFn = context.WithDeadline(context.Background(), time.Now().Add(
			time.Duration(200)*time.Millisecond))
		defer cancelFn()
	}
	f := NewWorkFeeder(ctx, logging.GetLogger(""), runtime.NumCPU(), work, withoutError)
	if !usingContext {
		time.AfterFunc(time.Duration(200)*time.Millisecond, func() { f.Close() })
	}
	done := make(chan bool)
	count := 0
	go func() {
		for d := range f.Adapt() {
			count++
			v, ok := d.R.(uint64)
			assert.Equal(t, true, ok)
			assert.Equal(t, true, v%2 == 0)
		}
		done <- true
	}()
	<-done
	assert.Equal(t, true, count > 0)
}

func testWithSingleGoroutineWithError(t *testing.T, usingContext bool) {
	var ctx = context.Background()
	var cancelFn context.CancelFunc
	if usingContext {
		ctx, cancelFn = context.WithDeadline(context.Background(), time.Now().Add(
			time.Duration(200)*time.Millisecond))
		defer cancelFn()
	}
	f := NewWorkFeeder(ctx, logging.GetLogger(""), runtime.NumCPU(), work, withError)
	if !usingContext {
		time.AfterFunc(time.Duration(200)*time.Millisecond, func() { f.Close() })
	}
	done := make(chan bool)
	count := 0
	go func() {
		for range f.Adapt() {
			count++
		}
		done <- true
	}()
	<-done
	assert.Equal(t, 0, count)
}

func testWithMultipleGoroutinesWithoutError(t *testing.T, usingContext bool) {
	var ctx = context.Background()
	var cancelFn context.CancelFunc
	if usingContext {
		ctx, cancelFn = context.WithDeadline(context.Background(), time.Now().Add(
			time.Duration(200)*time.Millisecond))
		defer cancelFn()
	}
	f := NewWorkFeeder(ctx, logging.GetLogger(""), runtime.NumCPU(), work, withoutError)
	if !usingContext {
		time.AfterFunc(time.Duration(200)*time.Millisecond, func() { f.Close() })
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
		v, ok := d.(uint64)
		assert.Equal(t, true, ok)
		assert.Equal(t, true, v%2 == 0)
	}

	assert.Equal(t, true, count > 0)
}

func testWithMultipleGoroutinesWithError(t *testing.T, usingContext bool) {
	var ctx = context.Background()
	var cancelFn context.CancelFunc
	if usingContext {
		ctx, cancelFn = context.WithDeadline(context.Background(), time.Now().Add(
			time.Duration(200)*time.Millisecond))
		defer cancelFn()
	}
	f := NewWorkFeeder(ctx, logging.GetLogger(""), runtime.NumCPU(), work, withError)
	if !usingContext {
		time.AfterFunc(time.Duration(200)*time.Millisecond, func() { f.Close() })
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
	for range out {
		count++
	}

	assert.Equal(t, 0, count)
}

func TestWorkFeederWithinSingleGoroutine(t *testing.T) {
	testWithSingleGoroutineWithError(t, true)
	testWithSingleGoroutineWithError(t, false)
	testWithSingleGoroutineWithoutError(t, true)
	testWithSingleGoroutineWithoutError(t, false)
}

func TestWorkFeederWithinMultipleGoroutines(t *testing.T) {
	testWithMultipleGoroutinesWithError(t, true)
	testWithMultipleGoroutinesWithError(t, false)
	testWithMultipleGoroutinesWithoutError(t, true)
	testWithMultipleGoroutinesWithoutError(t, false)
}
