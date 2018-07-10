package piezas

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/samwooo/bolsa/common/logging"
	"github.com/stretchr/testify/assert"
)

func TestWaterfallWithError(t *testing.T) {
	logging.DefaultLogger("", logging.LogLevelFromString("INFO"), 100)

	r := Waterfall(logging.GetLogger("waterfall test "), tasks{
		func(a ...int) (int, int, error) {
			total := 0
			for _, v := range a {
				total += v
			}
			return total, total, fmt.Errorf("Lucy&Lily")
		},
		func(a int, b int, e error) (int, error) {
			return a + b, e
		},
		func(a int, e error) (int, error) {
			assert.Equal(t, 12, a)
			assert.Equal(t, "Lucy&Lily", e.Error())
			return a, e
		}}, 1, 2, 3)
	assert.Equal(t, []interface{}{12, fmt.Errorf("Lucy&Lily")}, r)
}

func TestWaterfallWithVariadicParas(t *testing.T) {
	logging.DefaultLogger("", logging.LogLevelFromString("INFO"), 100)

	r := Waterfall(logging.GetLogger("waterfall test "), tasks{
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
		func(a int) int {
			assert.Equal(t, 362925, a)
			return a
		}}, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	assert.Equal(t, []interface{}{362925}, r)
}

func TestWaterfallWithFixedParas(t *testing.T) {
	logging.DefaultLogger("", logging.LogLevelFromString("INFO"), 100)

	r := Waterfall(logging.GetLogger("waterfall test "), tasks{
		func(a int) (int, string, error) {
			return a * a, strconv.Itoa(a), nil
		},
		func(a int, b string, e error) string {
			return strconv.Itoa(a) + b
		},
		func(a string) string {
			assert.Equal(t, "10000100", a)
			return a
		}}, 100)
	assert.Equal(t, []interface{}{"10000100"}, r)
}

func TestWaterfallWithCombination(t *testing.T) {
	logging.DefaultLogger("", logging.LogLevelFromString("INFO"), 100)
	logger := logging.GetLogger("waterfall test ")

	r := Waterfall(logger, tasks{
		func(a int) (int, string, error) {
			return a * a, strconv.Itoa(a), nil
		},
		func(a int, b string, e error) string {
			return strconv.Itoa(a) + b
		},
		func(a string) string {
			assert.Equal(t, "10000100", a)

			r := Waterfall(logger, tasks{
				func(a string) (string, error) {
					return a + a, nil
				},
				func(b string, e error) (int, error) {
					return strconv.Atoi(b)
				},
				func(a int, e error) (int, error) {
					assert.Equal(t, nil, e)
					assert.Equal(t, 1000010010000100, a)
					return a, e
				}}, a)
			assert.Equal(t, []interface{}{1000010010000100, nil}, r)
			return a
		}}, 100)
	assert.Equal(t, []interface{}{"10000100"}, r)
}
