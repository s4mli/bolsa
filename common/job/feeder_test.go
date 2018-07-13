package job

import (
	"runtime"
	"testing"

	"github.com/samwooo/bolsa/common"
	"github.com/stretchr/testify/assert"
)

func TestDataFeederWithinSingleGoroutine(t *testing.T) {
	var data []interface{}
	for k := 0; k < 100; k++ {
		data = append(data, k)
	}
	ds := NewDataFeeder(data)

	done := make(chan bool)
	count := 0
	go func() {
		for d := range ds.Adapt() {
			count++
			assert.Equal(t, true, common.IsIn(d.R, data))
		}
		done <- true
	}()
	<-done
	assert.Equal(t, len(data), count)
}

func TestDataFeederWithinMultipleGoroutines(t *testing.T) {
	var data []interface{}
	for k := 0; k < 100; k++ {
		data = append(data, k)
	}
	workers := runtime.NumCPU()
	ds := NewDataFeeder(data)
	done := make(chan bool, workers)
	out := make(chan interface{}, workers)
	count := 0
	for i := 0; i < workers; i++ {
		go func() {
			for d := range ds.Adapt() {
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
