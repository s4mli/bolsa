package unit_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/samwooo/bolsa/gadgets/piezas"
	"github.com/stretchr/testify/assert"
)

type _waterfallTester struct{}

func (anonymous *_waterfallTester) testWithError(t *testing.T) {
	// Error would NOT stop the waterfall but will be piped all the way down to the end ! ?
	piezas.NewWaterfall(piezas.Tasks{
		func(a ...int) (int, int, error) {
			return 0, 0, fmt.Errorf("Lucy&Lily")
		},
		func(a int, b int, e error) (int, error) {
			return a + b, e
		},
		func(a int, e error) {
			assert.Equal(t, 0, a)
			assert.Equal(t, "Lucy&Lily", e.Error())
		}}, 1, 2, 3)
}

func (anonymous *_waterfallTester) testWithVariadicParas(t *testing.T) {
	piezas.NewWaterfall(piezas.Tasks{
		func(a ...int) (int, int, error) {
			total := 1
			count := 0
			for _, arg := range a {
				total = total * arg
				count = count + arg
			}
			return total, count, nil
		},
		func(a int, b int, e error) int {
			return a + b
		},
		func(a int) {
			assert.Equal(t, 362925, a)
		}}, 1, 2, 3, 4, 5, 6, 7, 8, 9)
}

func (anonymous *_waterfallTester) testWithFixedParas(t *testing.T) {
	piezas.NewWaterfall(piezas.Tasks{
		func(a int) (int, string, error) {
			return a * a, strconv.Itoa(a), nil
		},
		func(a int, b string, e error) string {
			return strconv.Itoa(a) + b
		},
		func(a string) {
			assert.Equal(t, "10000100", a)
		}}, 100)
}

func (anonymous *_waterfallTester) testWithCombination(t *testing.T) {
	piezas.NewWaterfall(piezas.Tasks{
		func(a int) (int, string, error) {
			return a * a, strconv.Itoa(a), nil
		},
		func(a int, b string, e error) string {
			return strconv.Itoa(a) + b
		},
		func(a string) {
			assert.Equal(t, "10000100", a)

			piezas.NewWaterfall(piezas.Tasks{
				func(a string) (string, error) {
					return a + a, nil
				},
				func(b string, e error) (int, error) {
					return strconv.Atoi(b)
				},
				func(a int, e error) {
					assert.Equal(t, nil, e)
					assert.Equal(t, 1000010010000100, a)
				}}, a)
		}}, 100)
}

func TestWaterfall(t *testing.T) {
	anonymous := _waterfallTester{}
	anonymous.testWithError(t)
	anonymous.testWithFixedParas(t)
	anonymous.testWithCombination(t)
	anonymous.testWithVariadicParas(t)
}
