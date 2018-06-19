package piezas

import (
	"testing"

	"fmt"

	"context"

	"github.com/samwooo/bolsa/gadgets/logging"
	"github.com/samwooo/bolsa/gadgets/util"
	"github.com/stretchr/testify/assert"
)

var everyIte = func(k interface{}) (bool, error) {
	if v, ok := k.(int); ok {
		return v%2 == 0, nil
	} else {
		return false, fmt.Errorf("cast error")
	}
}

func testEveryWithError(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", util.APP_NAME),
		logging.LogLevelFromString("DEBUG"), 100)

	input := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, fmt.Errorf("a")}
	r := Every(context.Background(), logging.GetLogger("every test "), input, everyIte)
	assert.Equal(t, false, r)
}

func testEveryWithFalse(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", util.APP_NAME),
		logging.LogLevelFromString("DEBUG"), 100)

	input := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	r := Every(context.Background(), logging.GetLogger("every test "), input, everyIte)
	assert.Equal(t, false, r)
}

func testEveryWithTrue(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", util.APP_NAME),
		logging.LogLevelFromString("DEBUG"), 100)

	input := []interface{}{2, 4, 6, 8}
	r := Every(context.Background(), logging.GetLogger("every test "), input, everyIte)
	assert.Equal(t, true, r)
}

func TestEvery(t *testing.T) {
	testEveryWithError(t)
	testEveryWithFalse(t)
	testEveryWithTrue(t)
}
