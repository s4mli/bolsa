package logging

import (
	"fmt"
)

type MetricsInfo struct {
	Action   string
	TimeCost float64
}

type metricsFilter struct {
	threshold int32
}

func (f *metricsFilter) WorthEmit(lvl LogLevel, msg interface{}) (bool, interface{}) {
	switch m := msg.(type) {
	case MetricsInfo:
		return m.TimeCost*1000 >= float64(f.threshold), func() string {
			return fmt.Sprintf("METRIC: %s = %f", m.Action, m.TimeCost)
		}
	default:
		return true, msg
	}
}

func newMetricsFilter(threshold int32) *metricsFilter {
	return &metricsFilter{threshold}
}
