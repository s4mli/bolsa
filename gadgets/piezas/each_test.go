package piezas

import (
	"context"
	"testing"

	"fmt"

	"github.com/samwooo/bolsa/gadgets/logging"
	"github.com/samwooo/bolsa/gadgets/util"
	"github.com/stretchr/testify/assert"
)

func TestEach(t *testing.T) {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", util.APP_NAME),
		logging.LogLevelFromString("DEBUG"), 100)

	input := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 'a'}
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
			assert.Equal(t, "cast error", done.E.Error())
		} else {
			v, ok := done.P.(int)
			assert.Equal(t, true, ok)
			assert.Equal(t, v*2, done.R)
		}
	}
}
