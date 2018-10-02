package filter

import (
	"github.com/samwooo/bolsa/logging/model"
)

type levelFilter struct {
	minLevel model.LogLevel
}

func (h *levelFilter) WorthEmit(lvl model.LogLevel, msg interface{}) (bool, interface{}) {
	return h.minLevel <= lvl, msg
}

func NewLevelFilter(minLevel model.LogLevel) *levelFilter { return &levelFilter{minLevel} }
