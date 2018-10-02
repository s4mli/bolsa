package filter

import (
	"fmt"

	"github.com/samwooo/bolsa/logging/model"
)

type metricsInfo struct {
	action   string
	timeCost float64
}

func NewMetricsInfo(action string, timeCost float64) *metricsInfo {
	return &metricsInfo{action, timeCost}
}

type metricsFilter struct{ threshold int32 }

func (f *metricsFilter) WorthEmit(lvl model.LogLevel, msg interface{}) (bool, interface{}) {
	switch m := msg.(type) {
	case *metricsInfo:
		return m.timeCost*1000 >= float64(f.threshold), func() string {
			return fmt.Sprintf("%s ⌚ %.1fs ( ≥ %dms )", m.action, m.timeCost, f.threshold)
		}
	default:
		return true, msg
	}
}

func NewMetricsFilter(threshold int32) *metricsFilter { return &metricsFilter{threshold} }
