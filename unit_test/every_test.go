package unit_test

import (
	"testing"

	"github.com/samwooo/bolsa/gadgets/piezas"
	"github.com/stretchr/testify/assert"
)

type _everyTester struct{}

func (anonymous *_everyTester) testWithError(t *testing.T) {
	// Type mismatched then do nothing
	input := []int64{1, 2, 3, 4, 5, 6, 7, 8, 99}
	r := piezas.Every(input,
		func(k int) int {
			return k * k
		})
	assert.Equal(t, false, r)

	r = piezas.Every(input,
		func(k int) bool {
			return true
		})
	assert.Equal(t, false, r)

	r = piezas.Every(input,
		func(k int) (bool, error) {
			return true, nil
		})
	assert.Equal(t, false, r)

	r = piezas.Every(input,
		func(k, j int) bool {
			return true
		})
	assert.Equal(t, false, r)
}

func (anonymous *_everyTester) testWithArray(t *testing.T) {
	r := piezas.Every([9]int64{1, 2, 3, 4, 5, 6, 7, 8, 99},
		func(k int64) bool {
			return k < 100
		})
	assert.Equal(t, true, r)

	r = piezas.Every([9]int64{1, 2, 3, 4, 5, 6, 7, 8, 99},
		func(k int64) bool {
			return k < 0
		})
	assert.Equal(t, false, r)
}

func (anonymous *_everyTester) testWithSlice(t *testing.T) {
	r := piezas.Every([]int64{1, 2, 3, 4, 5, 6, 7, 8, 99},
		func(k int64) bool {
			return k < 100
		})
	assert.Equal(t, true, r)

	r = piezas.Every([]int64{1, 2, 3, 4, 5, 6, 7, 8, 99},
		func(k int64) bool {
			return k < 0
		})
	assert.Equal(t, false, r)
}

func TestEvery(t *testing.T) {
	anonymous := _everyTester{}
	anonymous.testWithError(t)
	anonymous.testWithSlice(t)
	anonymous.testWithArray(t)
}
