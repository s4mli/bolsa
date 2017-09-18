package unit_test

import (
	"sync"
	"testing"

	"github.com/samwooo/bolsa/gadgets/piezas"
	"github.com/stretchr/testify/assert"
)

type _eachTester struct{}

func (anonymous *_eachTester) testWithError(t *testing.T) {
	// Type mismatched then do nothing
	input := []int64{1, 2, 3, 4, 5, 6, 7, 8, 99}
	callCount := 0
	mutex := &sync.Mutex{}
	piezas.Each(input,
		func(k int) {
			mutex.Lock()
			callCount++
			mutex.Unlock()
		})
	assert.Equal(t, 0, callCount)

	piezas.Each(input,
		func(k int) bool {
			mutex.Lock()
			callCount++
			mutex.Unlock()
			return true
		})
	assert.Equal(t, 0, callCount)

	piezas.Each(input,
		func(k int) (bool, error) {
			mutex.Lock()
			callCount++
			mutex.Unlock()
			return true, nil
		})
	assert.Equal(t, 0, callCount)

	piezas.Each(input,
		func(k, j int) bool {
			mutex.Lock()
			callCount++
			mutex.Unlock()
			return true
		})
	assert.Equal(t, 0, callCount)
}

func (anonymous *_eachTester) testWithArray(t *testing.T) {
	callCount := 0
	input := [9]int64{1, 2, 3, 4, 5, 6, 7, 8, 99}
	mutex := &sync.Mutex{}
	piezas.Each(input,
		func(k int64) {
			mutex.Lock()
			callCount++
			mutex.Unlock()
		})
	assert.Equal(t, len(input), callCount)
}

func (anonymous *_eachTester) testWithSlice(t *testing.T) {
	input := []int64{1, 2, 3, 4, 5, 6, 7, 8, 99}
	callCount := 0
	mutex := &sync.Mutex{}
	piezas.Each(input,
		func(k int64) {
			mutex.Lock()
			callCount++
			mutex.Unlock()
		})
	assert.Equal(t, len(input), callCount)
}

func TestEach(t *testing.T) {
	anonymous := _eachTester{}
	anonymous.testWithError(t)
	anonymous.testWithSlice(t)
	anonymous.testWithArray(t)
}
