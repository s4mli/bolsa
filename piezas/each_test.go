package piezas

import (
	"context"
	"fmt"
	"testing"

	"github.com/samwooo/bolsa/job/model"
	"github.com/samwooo/bolsa/logging"
	logModel "github.com/samwooo/bolsa/logging/model"
	"github.com/stretchr/testify/assert"
)

func TestEach(t *testing.T) {
	logging.DefaultLogger("", logModel.LogLevelFromString("ERROR"), 100)
	input := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, "abc"}
	r := Each(context.Background(), input,
		func(k interface{}) (interface{}, error) {
			if v, ok := k.(int); ok {
				return v * 2, nil
			} else {
				return nil, fmt.Errorf("cast error")
			}
		})
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(model.Done)
		assert.Equal(t, true, ok)
		if done.E != nil {
			assert.Equal(t, "✗ labor failed ( abc, cast error )", done.E.Error())
		} else {
			v, ok := done.D.(int)
			assert.Equal(t, true, ok)
			assert.Equal(t, v*2, done.R)
		}
		return true
	})
}
