package piezas

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type eachTester struct{}

func (anonymous *eachTester) testWithError(t *testing.T) {
	// Type mismatched then do nothing
	input := []int64{1, 2, 3, 4, 5, 6, 7, 8, 99}
	callCount := 0
	mutex := &sync.Mutex{}
	Each(input,
		func(k int) {
			mutex.Lock()
			callCount++
			mutex.Unlock()
		})
	assert.Equal(t, 0, callCount)

	Each(input,
		func(k int) bool {
			mutex.Lock()
			callCount++
			mutex.Unlock()
			return true
		})
	assert.Equal(t, 0, callCount)

	Each(input,
		func(k int) (bool, error) {
			mutex.Lock()
			callCount++
			mutex.Unlock()
			return true, nil
		})
	assert.Equal(t, 0, callCount)

	Each(input,
		func(k, j int) bool {
			mutex.Lock()
			callCount++
			mutex.Unlock()
			return true
		})
	assert.Equal(t, 0, callCount)
}

func (anonymous *eachTester) testWithArray(t *testing.T) {
	callCount := 0
	input := [9]int64{1, 2, 3, 4, 5, 6, 7, 8, 99}
	mutex := &sync.Mutex{}
	Each(input,
		func(k int64) {
			mutex.Lock()
			callCount++
			mutex.Unlock()
		})
	assert.Equal(t, len(input), callCount)
}

func (anonymous *eachTester) testWithSlice(t *testing.T) {
	input := []int64{}
	for i := 0; i < 999999; i++ {
		input = append(input, int64(i))
	}
	callCount := 0
	mutex := &sync.Mutex{}
	Each(input,
		func(k int64) {
			mutex.Lock()
			callCount++
			mutex.Unlock()
		})
	assert.Equal(t, len(input), callCount)
}

func TestEach(t *testing.T) {
	anonymous := eachTester{}
	anonymous.testWithError(t)
	anonymous.testWithSlice(t)
	anonymous.testWithArray(t)
}
