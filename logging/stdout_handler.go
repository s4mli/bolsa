package logging

import (
	"fmt"
	"log"
	"os"
)

type stdoutHandler struct {
	log *log.Logger
}

func (h *stdoutHandler) Emit(lvl LogLevel, prefix string, msg interface{}) {
	switch e := msg.(type) {
	case string:
		h.log.Println(lvl.String() + ": " + prefix + e)
	case error:
		h.log.Println(lvl.String() + ": " + prefix + e.Error())
	case func() string:
		h.log.Println(lvl.String() + ": " + prefix + e())
	default:
		panic(fmt.Errorf("unhandled msg type: %v", e))
	}
}

func NewStdoutHandler() Handler {
	return &stdoutHandler{log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)}
}
