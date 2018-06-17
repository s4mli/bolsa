package render

import (
	"github.com/samwooo/bolsa/gadgets/endpoints/restful/render/engine"
)

var (
	engines = make(map[string]engine.Engine, 8)
)

func Register(name string, e engine.Engine) {
	if e == nil {
		panic("render: Register engine is nil")
	}
	if _, ok := engines[name]; ok {
		panic("render: Register called twice for driver " + name)
	}
	engines[name] = e
}

func renderBy(e engine.Engine, data interface{}) interface{} {
	return e.Render(data)
}

func Render(name string, data interface{}) interface{} {
	if e, ok := engines[name]; !ok {
		panic("render: Unregistered engine " + name)
	} else {
		return renderBy(e, data)
	}
}
