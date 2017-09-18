package unit_test

import (
	"testing"

	"github.com/samwooo/bolsa/gadgets/piezas"
	"github.com/stretchr/testify/assert"
)

type _filterTester struct{}

func (anonymous *_filterTester) testWithError(t *testing.T) {
	input := []int64{1, 2, 3, 4, 5, 6, 7, 8, 99}
	r := piezas.Filter(input,
		func(k int) int {
			return k * k
		}, false)
	assert.Equal(t, 0, len(r))

	r = piezas.Filter(input,
		func(k int) bool {
			return true
		}, false)
	assert.Equal(t, 0, len(r))

	r = piezas.Filter(input,
		func(k int) (bool, error) {
			return true, nil
		}, false)
	assert.Equal(t, 0, len(r))

	r = piezas.Filter(input,
		func(k, j int) bool {
			return true
		}, false)
	assert.Equal(t, 0, len(r))
}

func (anonymous *_filterTester) testWithArray(t *testing.T) {
	r := piezas.Filter([9]int64{1, 2, 3, 4, 5, 6, 7, 8, 99},
		func(k int64) bool {
			return k > 0
		}, true)

	expected := [9]int64{1, 2, 3, 4, 5, 6, 7, 8, 99}
	assert.Equal(t, len(expected), len(r))
	for index, v := range expected {
		assert.Equal(t, v, r[index])
	}

	r = piezas.Filter([9]int64{1, 2, 3, 4, 5, 6, 7, 8, 99},
		func(k int64) bool {
			return k > 0
		}, false)

	assert.Equal(t, len(expected), len(r))
	for _, actual := range r {
		found := false
		for _, v := range expected {
			if v == actual {
				found = true
				break
			}
		}
		assert.Equal(t, true, found)
	}
}

func (anonymous *_filterTester) testWithSlice(t *testing.T) {
	r := piezas.Filter([]int64{1, 2, 3, 4, 5, 6, 7, 8, 99},
		func(k int64) bool {
			return k > 0
		}, true)

	expected := []int64{1, 2, 3, 4, 5, 6, 7, 8, 99}
	assert.Equal(t, len(expected), len(r))
	for index, v := range expected {
		assert.Equal(t, v, r[index])
	}

	r = piezas.Filter([]int64{1, 2, 3, 4, 5, 6, 7, 8, 99},
		func(k int64) bool {
			return k > 0
		}, false)

	assert.Equal(t, len(expected), len(r))
	for _, actual := range r {
		found := false
		for _, v := range expected {
			if v == actual {
				found = true
				break
			}
		}
		assert.Equal(t, true, found)
	}
}

func (anonymous *_filterTester) testWithSinglePara(t *testing.T) {
	r := piezas.Filter(99,
		func(k int) bool {
			return k > 0
		}, true)

	expected := []int{99}
	assert.Equal(t, len(expected), len(r))
	for index, v := range expected {
		assert.Equal(t, v, r[index])
	}

	r = piezas.Filter([]int{99},
		func(k int) bool {
			return k > 0
		}, false)

	assert.Equal(t, len(expected), len(r))
}

func TestFilter(t *testing.T) {
	anonymous := _filterTester{}
	anonymous.testWithError(t)
	anonymous.testWithSlice(t)
	anonymous.testWithArray(t)
	anonymous.testWithSinglePara(t)
}
