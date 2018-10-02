package model

import (
	"fmt"
	"strings"
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
		return " ☻ "
	case INFO:
		return " ☺ "
	case WARN:
		return " ☹ "
	case ERROR:
		return " ☠ "
	default:
		panic(fmt.Errorf("unknown log level"))
	}
}
