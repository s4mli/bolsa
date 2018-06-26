package piezas

import (
	"context"
	"fmt"
	"testing"

	"bitbucket.org/ecal/hookloop/common/util"
	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/common/logging"
	"github.com/stretchr/testify/assert"
)

var mapIte = func(k interface{}) (interface{}, error) {
	if v, ok := k.(int); ok {
		return v + 1, nil
	} else {
		return nil, fmt.Errorf("cast error")
	}
}

func testMapWithError(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", common.APP_NAME),
		logging.LogLevelFromString("DEBUG"), 100)

	input := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, "abc"}
	r := Map(context.Background(), logging.GetLogger("map test "), input, mapIte)
	assert.Equal(t, len(input)-1, len(r))
	for _, v := range input {
		if vi, ok := v.(int); ok {
			assert.Equal(t, true, common.IsIn(vi+1, r))
		}
	}
}

func testMapWithoutError(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", util.APP_NAME),
		logging.LogLevelFromString("DEBUG"), 100)

	input := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	r := Map(context.Background(), logging.GetLogger("map test "), input, mapIte)
	assert.Equal(t, len(input), len(r))
	for _, v := range input {
		vi, _ := v.(int)
		assert.Equal(t, true, common.IsIn(vi+1, r))
	}
}

func TestMap(t *testing.T) {
	testMapWithError(t)
	testMapWithoutError(t)
}
