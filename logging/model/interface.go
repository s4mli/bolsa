package model

import (
	"time"
)

type Logger interface {
	Debug(interface{})
	Info(interface{})
	Warn(interface{})
	Error(interface{})
	Metrics(string, time.Time)

	Debugf(string, ...interface{})
	Infof(string, ...interface{})
	Warnf(string, ...interface{})
	Errorf(string, ...interface{})

	GetChild(string) Logger
}

type Filter interface {
	WorthEmit(LogLevel, interface{}) (bool, interface{})
}

type Handler interface {
	Emit(LogLevel, string, interface{})
}
