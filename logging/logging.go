package logging

import (
	"fmt"
	"strings"
	"time"
)

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

func LogLevelFromString(s string) LogLevel {
	switch strings.ToLower(s) {
	case "debug":
		return DEBUG
	case "info":
		return INFO
	case "warn":
		return WARN
	case "error":
		return ERROR
	default:
		return INFO
	}
}

func (l LogLevel) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		panic(fmt.Errorf("unknown log level"))
	}
}

type logger struct {
	prefix  string
	filters []Filter
	handler Handler
}

func (l *logger) Debug(msg interface{}) { l.message(DEBUG, msg) }
func (l *logger) Info(msg interface{})  { l.message(INFO, msg) }
func (l *logger) Warn(msg interface{})  { l.message(WARN, msg) }
func (l *logger) Error(msg interface{}) { l.message(ERROR, msg) }

func (l *logger) Debugf(f string, a ...interface{}) { l.Debug(fmt.Sprintf(f, a...)) }
func (l *logger) Infof(f string, a ...interface{})  { l.Info(fmt.Sprintf(f, a...)) }
func (l *logger) Warnf(f string, a ...interface{})  { l.Warn(fmt.Sprintf(f, a...)) }
func (l *logger) Errorf(f string, a ...interface{}) { l.Error(fmt.Sprintf(f, a...)) }

func (l *logger) filter(lvl LogLevel, msg interface{}) (bool, interface{}) {
	for _, f := range l.filters {
		if worthy, m := f.WorthEmit(lvl, msg); !worthy {
			return false, nil
		} else {
			msg = m
		}
	}
	return true, msg
}

func (l *logger) message(lvl LogLevel, msg interface{}) {
	if worthy, msg := l.filter(lvl, msg); worthy && l.handler != nil {
		l.handler.Emit(lvl, l.prefix, msg)
	}
}

func (l logger) GetChild(prefix string) Logger {
	l.prefix = l.prefix + prefix
	return &l
}

var rootLogger *logger = &logger{}

func SetupLogger(prefix string, h Handler, f ...Filter) Logger {
	rootLogger.prefix = prefix
	rootLogger.handler = h
	rootLogger.filters = append(rootLogger.filters, f...)
	return rootLogger.GetChild("")
}

func DefaultLogger(prefix string, lvl LogLevel, metricThreshold int32) Logger {
	return SetupLogger(prefix, NewStdoutHandler(), newLevelFilter(lvl),
		newMetricsFilter(metricThreshold))
}

func GetLogger(context string) Logger {
	return rootLogger.GetChild(context)
}

// log slow actions such as db query etc.
func LogMetrics(logger Logger, action string, start time.Time) {
	logger.Warn(MetricsInfo{
		Action:   action,
		TimeCost: time.Since(start).Seconds(),
	})
}
