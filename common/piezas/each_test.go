package piezas

import (
	"context"
	"fmt"
	"testing"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/common/logging"
	"github.com/stretchr/testify/assert"
)

func TestEach(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", common.APP_NAME),
		logging.LogLevelFromString("DEBUG"), 100)

	input := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, "abc"}
	r := Each(context.Background(), logging.GetLogger("each test "),
		input,
		func(k interface{}) (interface{}, error) {
			if v, ok := k.(int); ok {
				return v * 2, nil
			} else {
				return nil, fmt.Errorf("cast error")
			}
		})
	assert.Equal(t, len(input), len(r))
	for _, done := range r {
		if done.E != nil {
			assert.Equal(t, "× labor failed ( abc, cast error )", done.E.Error())
		} else {
			v, ok := done.P.(int)
			assert.Equal(t, true, ok)
			assert.Equal(t, v*2, done.R)
		}
	}
}
