package job

import (
	"context"
	"runtime"
	"testing"

	"github.com/samwooo/bolsa/common"
	"github.com/stretchr/testify/assert"
)

func TestDataSupplierWithinSingleGoroutine(t *testing.T) {
	var data []interface{}
	for k := 0; k < 100; k++ {
		data = append(data, k)
	}
	ds := NewDataSupplier(data)
	done := make(chan bool)
	count := 0
	go func() {
		for {
			if d, ok := ds.Drain(context.Background()); ok {
				assert.Equal(t, true, common.IsIn(d, data))
				count++
			} else {
				break
			}
		}
		done <- true
	}()
	<-done
	assert.Equal(t, len(data), count)
}

func TestDataSupplierWithinMultipleGoroutines(t *testing.T) {
	var data []interface{}
	for k := 0; k < 100; k++ {
		data = append(data, k)
	}
	workers := runtime.NumCPU()
	ds := NewDataSupplier(data)
	done := make(chan bool, workers)
	out := make(chan interface{}, workers)
	count := 0
	for i := 0; i < workers; i++ {
		go func() {
			for {
				if d, ok := ds.Drain(context.Background()); ok {
					assert.Equal(t, true, common.IsIn(d, data))
					out <- d
				} else {
					break
				}
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
