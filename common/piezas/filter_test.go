package piezas

import (
	"context"
	"fmt"
	"testing"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/common/logging"
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
	logging.DefaultLogger("", logging.LogLevelFromString("INFO"), 100)

	input := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, "abc"}
	r := Filter(context.Background(), logging.GetLogger("test "), input, filterIte)
	for _, k := range []interface{}{2, 4, 6, 8} {
		assert.Equal(t, true, common.IsIn(k, r))
	}
}

func testFilterWithoutError(t *testing.T) {
	logging.DefaultLogger("", logging.LogLevelFromString("INFO"), 100)

	input := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	r := Filter(context.Background(), logging.GetLogger("test "), input, filterIte)
	for _, k := range []interface{}{2, 4, 6, 8} {
		assert.Equal(t, true, common.IsIn(k, r))
	}
}

func TestFilter(t *testing.T) {
	testFilterWithError(t)
	testFilterWithoutError(t)
}
