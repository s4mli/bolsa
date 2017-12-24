package logging

type Logger interface {
	Debug(msg interface{})
	Info(msg interface{})
	Warn(msg interface{})
	Err(msg interface{})

	Debugf(f string, a ...interface{})
	Infof(f string, a ...interface{})
	Warnf(f string, a ...interface{})
	Errf(f string, a ...interface{})

	GetChild(prefix string) Logger
}

type Filter interface {
	WorthEmit(lvl LogLevel, msg interface{}) (bool, interface{})
}

type Handler interface {
	Emit(lvl LogLevel, prefix string, msg interface{})
}
