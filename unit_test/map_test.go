package unit_test

import (
	"bolsa/gadgets/piezas"
	"testing"

	"github.com/stretchr/testify/assert"
)

type _mapTester struct{}

func (anonymous *_mapTester) testWithError(t *testing.T) {
	// Type mismatched then do nothing
	input := []int64{1, 2, 3, 4, 5, 6, 7, 8, 99}
	r := piezas.Map(input,
		func(k int) int {
			return k * k
		}, true)
	assert.Equal(t, len(input), len(r))
	for index, v := range input {
		assert.Equal(t, v, r[index])
	}
}

func (anonymous *_mapTester) testWithArray(t *testing.T) {
	r := piezas.Map([9]int64{1, 2, 3, 4, 5, 6, 7, 8, 99},
		func(k int64) int64 {
			return k * k
		}, true)

	expected := [9]int64{1, 4, 9, 16, 25, 36, 49, 64, 9801}
	assert.Equal(t, len(expected), len(r))
	for index, v := range expected {
		assert.Equal(t, v, r[index])
	}

	r = piezas.Map([9]int64{1, 2, 3, 4, 5, 6, 7, 8, 99},
		func(k int64) int64 {
			return k * k
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

func (anonymous *_mapTester) testWithSlice(t *testing.T) {
	r := piezas.Map([]int64{1, 2, 3, 4, 5, 6, 7, 8, 99},
		func(k int64) int64 {
			return k * k
		}, true)

	expected := []int64{1, 4, 9, 16, 25, 36, 49, 64, 9801}
	assert.Equal(t, len(expected), len(r))
	for index, v := range expected {
		assert.Equal(t, v, r[index])
	}

	r = piezas.Map([]int64{1, 2, 3, 4, 5, 6, 7, 8, 99},
		func(k int64) int64 {
			return k * k
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

func (anonymous *_mapTester) testWithSinglePara(t *testing.T) {
	r := piezas.Map(99,
		func(k int) int {
			return k * k
		}, true)

	expected := []int{9801}
	assert.Equal(t, len(expected), len(r))
	for index, v := range expected {
		assert.Equal(t, v, r[index])
	}

	r = piezas.Map([]int{99},
		func(k int) int {
			return k * k
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

func TestMap(t *testing.T) {
	anonymous := _mapTester{}
	anonymous.testWithError(t)
	anonymous.testWithSlice(t)
	anonymous.testWithArray(t)
	anonymous.testWithSinglePara(t)
}
