package logging

import (
	"time"

	"github.com/samwooo/bolsa/logging/model"
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
	WorthEmit(model.LogLevel, interface{}) (bool, interface{})
}

type Handler interface {
	Emit(model.LogLevel, string, interface{})
}
