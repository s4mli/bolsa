package model

import (
	"fmt"
)

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

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
