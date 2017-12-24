package logging

type levelFilter struct {
	minLevel LogLevel
}

func (h *levelFilter) WorthEmit(lvl LogLevel, msg interface{}) (bool, interface{}) {
	return h.minLevel <= lvl, msg
}

func newLevelFilter(minLevel LogLevel) *levelFilter {
	return &levelFilter{minLevel}
}
