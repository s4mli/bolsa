package handler

import (
	"log"
	"os"

	"github.com/samwooo/bolsa/logging/model"
)

type stdoutHandler struct {
	log *log.Logger
}

func (h *stdoutHandler) Emit(lvl model.LogLevel, prefix string, msg interface{}) {
	switch e := msg.(type) {
	case string:
		h.log.Println(lvl.String() + prefix + ": " + e)
	case error:
		h.log.Println(lvl.String() + prefix + ": " + e.Error())
	case func() string:
		h.log.Println(lvl.String() + prefix + ": " + e())
	default:
		h.log.Printf("%s%s: %+v\n", lvl.String(), prefix, msg)
	}
}

func NewStdoutHandler() *stdoutHandler {
	return &stdoutHandler{log.New(os.Stdout, "", log.LstdFlags)}
}
