package logging

import (
	"fmt"
	"time"

	"github.com/samwooo/bolsa/logging/filter"
	"github.com/samwooo/bolsa/logging/handler"
	"github.com/samwooo/bolsa/logging/model"
)

type logger struct {
	prefix  string
	filters []model.Filter
	handler model.Handler
}

func (l *logger) Debug(msg interface{}) { l.message(model.DEBUG, msg) }
func (l *logger) Info(msg interface{})  { l.message(model.INFO, msg) }
func (l *logger) Warn(msg interface{})  { l.message(model.WARN, msg) }
func (l *logger) Error(msg interface{}) { l.message(model.ERROR, msg) }
func (l *logger) Metrics(action string, start time.Time) {
	l.Warn(filter.NewMetricsInfo(action, time.Since(start).Seconds()))
}

func (l *logger) Debugf(f string, a ...interface{}) { l.Debug(fmt.Sprintf(f, a...)) }
func (l *logger) Infof(f string, a ...interface{})  { l.Info(fmt.Sprintf(f, a...)) }
func (l *logger) Warnf(f string, a ...interface{})  { l.Warn(fmt.Sprintf(f, a...)) }
func (l *logger) Errorf(f string, a ...interface{}) { l.Error(fmt.Sprintf(f, a...)) }

func (l *logger) filter(lvl model.LogLevel, msg interface{}) (bool, interface{}) {
	for _, f := range l.filters {
		if worthy, m := f.WorthEmit(lvl, msg); !worthy {
			return false, nil
		} else {
			msg = m
		}
	}
	return true, msg
}

func (l *logger) message(lvl model.LogLevel, msg interface{}) {
	if worthy, msg := l.filter(lvl, msg); worthy && l.handler != nil {
		l.handler.Emit(lvl, l.prefix, msg)
	}
}

func (l logger) GetChild(prefix string) model.Logger {
	l.prefix = l.prefix + "➟" + prefix
	return &l
}

var rootLogger = &logger{}

func SetupLogger(prefix string, h model.Handler, f ...model.Filter) model.Logger {
	rootLogger.prefix = prefix
	rootLogger.handler = h
	rootLogger.filters = append(rootLogger.filters, f...)
	return rootLogger
}

func DefaultLogger(prefix string, lvl model.LogLevel, metricThreshold int32) model.Logger {
	return SetupLogger(
		prefix,
		handler.NewStdoutHandler(),
		filter.NewLevelFilter(lvl),
		filter.NewMetricsFilter(metricThreshold))
}

func GetLogger(prefix string) model.Logger { return rootLogger.GetChild(prefix) }
