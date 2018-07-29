package job

import (
	"context"
	"runtime"
	"testing"
	"time"

	"sync/atomic"

	"github.com/samwooo/bolsa/common/logging"
	"github.com/stretchr/testify/assert"
)

var _ = logging.DefaultLogger("", logging.LogLevelFromString("ERROR"), 100)

var COUNTER uint64 = 0

type laborImp struct{}

func (l *laborImp) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	return atomic.AddUint64(&COUNTER, 2), nil
}

func testWithSingleGoroutine(t *testing.T, usingContext bool) {
	var ctx = context.Background()
	var cancelFn context.CancelFunc
	if usingContext {
		ctx, cancelFn = context.WithDeadline(context.Background(), time.Now().Add(
			time.Duration(200)*time.Millisecond))
		defer cancelFn()
	}
	f := NewChanFeeder(ctx, logging.GetLogger(""), &laborImp{})
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

func testWithMultipleGoroutines(t *testing.T, usingContext bool) {
	var ctx = context.Background()
	var cancelFn context.CancelFunc
	if usingContext {
		ctx, cancelFn = context.WithDeadline(context.Background(), time.Now().Add(
			time.Duration(200)*time.Millisecond))
		defer cancelFn()
	}
	f := NewChanFeeder(ctx, logging.GetLogger(""), &laborImp{})
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

func TestChanFeederWithinSingleGoroutine(t *testing.T) {
	testWithSingleGoroutine(t, true)
	testWithSingleGoroutine(t, false)
}

func TestChanFeederWithinMultipleGoroutinesWithContext(t *testing.T) {
	testWithMultipleGoroutines(t, true)
	testWithMultipleGoroutines(t, false)
}
