package piezas

import (
	"testing"

	"fmt"

	"context"

	"github.com/samwooo/bolsa/gadgets/logging"
	"github.com/samwooo/bolsa/gadgets/util"
	"github.com/stretchr/testify/assert"
)

var filterIte = func(k interface{}) (bool, error) {
	if v, ok := k.(int); ok {
		return v%2 == 0, nil
	} else {
		return false, fmt.Errorf("cast error")
	}
}

func testFilterWithError(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", util.APP_NAME),
		logging.LogLevelFromString("DEBUG"), 100)

	input := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, "a"}
	r := Filter(context.Background(), logging.GetLogger("filter test "), input, filterIte)
	for _, k := range []interface{}{2, 4, 6, 8} {
		assert.Equal(t, true, util.IsIn(k, r))
	}
}

func testFilterWithoutError(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", util.APP_NAME),
		logging.LogLevelFromString("DEBUG"), 100)

	input := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	r := Filter(context.Background(), logging.GetLogger("filter test "), input, filterIte)
	for _, k := range []interface{}{2, 4, 6, 8} {
		assert.Equal(t, true, util.IsIn(k, r))
	}
}

func TestFilter(t *testing.T) {
	testFilterWithError(t)
	testFilterWithoutError(t)
}
