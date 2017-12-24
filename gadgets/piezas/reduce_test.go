package piezas

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

type _reduceTester struct{}

func (anonymous *_reduceTester) testWithError(t *testing.T) {
	// Type mismatched then do nothing
	input := []int64{1, 2, 3, 4, 5, 6, 7, 8, 99}
	r := Reduce(input, 0,
		func(k int) int {
			return 1
		})
	assert.Equal(t, 0, r)

	r = Reduce(input, 0,
		func(k int, m int) int {
			return k + m
		})
	assert.Equal(t, 0, r)

	r = Reduce(input, 0,
		func(k int, m int) int {
			return k + m
		})
	assert.Equal(t, 0, r)

	r = Reduce(input, 0,
		func(k int, m int) (int, error) {
			return k + m, nil
		})
	assert.Equal(t, 0, r)
}

func (anonymous *_reduceTester) testWithArray(t *testing.T) {
	input := [9]int{1, 2, 3, 4, 5, 6, 7, 8, 99}
	expected := 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 99
	r := Reduce(input, 0,
		func(k, memo int) int {
			return memo + k
		})
	assert.Equal(t, expected, r)

	r = Reduce([9]string{"1", "2", "3", "4", "5", "6", "7", "8", "99"}, 0,
		func(k string, memo int) int {
			r, err := strconv.Atoi(k)
			if err != nil {
				return memo
			} else {
				return memo + r
			}
		})
	assert.Equal(t, expected, r)
}

func (anonymous *_reduceTester) testWithSlice(t *testing.T) {
	input := []int{1, 2, 3, 4, 5, 6, 7, 8, 99}
	expected := 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 99
	r := Reduce(input, 0,
		func(k, memo int) int {
			return memo + k
		})
	assert.Equal(t, expected, r)

	r = Reduce([]string{"1", "2", "3", "4", "5", "6", "7", "8", "99"}, 0,
		func(k string, memo int) int {
			r, err := strconv.Atoi(k)
			if err != nil {
				return memo
			} else {
				return memo + r
			}
		})
	assert.Equal(t, expected, r)
}

func (anonymous *_reduceTester) testWithSinglePara(t *testing.T) {
	input := 99
	expected := 99
	r := Reduce(input, 0,
		func(k, memo int) int {
			return memo + k
		})
	assert.Equal(t, expected, r)

	r = Reduce("99", 0,
		func(k string, memo int) int {
			r, err := strconv.Atoi(k)
			if err != nil {
				return memo
			} else {
				return memo + r
			}
		})
	assert.Equal(t, expected, r)
}

func TestReduce(t *testing.T) {
	anonymous := _reduceTester{}
	anonymous.testWithError(t)
	anonymous.testWithSlice(t)
	anonymous.testWithArray(t)
	anonymous.testWithSinglePara(t)
}
