package piezas

import (
	"context"
	"testing"

	"fmt"

	"github.com/samwooo/bolsa/gadgets/logging"
	"github.com/samwooo/bolsa/gadgets/util"
	"github.com/stretchr/testify/assert"
)

var reduceIte = func(k interface{}, memo interface{}) (interface{}, error) {
	v, vok := k.(int)
	m, mok := memo.(int)
	if vok && mok {
		return v + m, nil
	} else {
		return m, fmt.Errorf("cast %+v error", k)
	}
}

func testReduceWithSingleError(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", util.APP_NAME),
		logging.LogLevelFromString("DEBUG"), 100)

	input := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, "a"}
	memo := 0
	r, err := Reduce(context.Background(), logging.GetLogger("reduce test "), input, memo, reduceIte)
	assert.Equal(t, 36, r)
	assert.Equal(t, "cast a error|", err.Error())
}

func testReduceWithMulipleError(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", util.APP_NAME),
		logging.LogLevelFromString("DEBUG"), 100)

	input := []interface{}{
		1, 2, 3, 4, 5, 6, 7, 8,
		1, 2, 3, 4, 5, 6, 7, 8,
		1, 2, 3, 4, 5, 6, 7, 8,
		1, 2, 3, 4, 5, 6, 7, 8,
		1, 2, 3, 4, 5, 6, 7, 8,
		"a", "b", "c",
	}
	memo := 0
	r, err := Reduce(context.Background(), logging.GetLogger("reduce test "), input, memo, reduceIte)
	assert.Equal(t, 180, r)
	assert.Equal(t, "cast a error|cast b error|cast c error|", err.Error())
}

func testReduceWithoutError(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", util.APP_NAME),
		logging.LogLevelFromString("DEBUG"), 100)

	input := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	memo := 0
	r, err := Reduce(context.Background(), logging.GetLogger("reduce test "), input, memo, reduceIte)
	assert.Equal(t, 36, r)
	assert.Equal(t, nil, err)
}

func TestReduce(t *testing.T) {
	testReduceWithSingleError(t)
	testReduceWithMulipleError(t)
	testReduceWithoutError(t)

}
