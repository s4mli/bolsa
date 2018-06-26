package piezas

import (
	"fmt"
	"reflect"
	"time"

	"github.com/samwooo/bolsa/common/logging"
)

type task interface{}
type tasks []task

func runTask(logger logging.Logger, t task, in <-chan []reflect.Value) <-chan []reflect.Value {
	runTaskOn := func(args []reflect.Value) []reflect.Value {
		fn := reflect.Indirect(reflect.ValueOf(t))
		if fn.Kind() != reflect.Func {
			err := fmt.Errorf("task ( %+v ) must be a func", t)
			logger.Error(err.Error())
			return []reflect.Value{reflect.ValueOf(err)}
		} else {
			return fn.Call(args)
		}
	}

	out := make(chan []reflect.Value)
	go func() {
		for data := range in {
			out <- runTaskOn(data)
		}
		close(out)
	}()
	return out
}

func run(in chan<- []reflect.Value, out <-chan []reflect.Value,
	firstArgs ...interface{}) []interface{} {

	go func() {
		var args []reflect.Value
		for _, arg := range firstArgs {
			args = append(args, reflect.ValueOf(arg))
		}
		in <- args
		close(in)
	}()

	var results []interface{}
	for _, v := range <-out {
		results = append(results, v.Interface())
	}
	return results
}

func Waterfall(logger logging.Logger, ts tasks, firstArgs ...interface{}) []interface{} {
	start := time.Now()
	in := make(chan []reflect.Value)
	var out <-chan []reflect.Value
	for _, t := range ts {
		if out == nil {
			out = runTask(logger, t, in)
		} else {
			out = runTask(logger, t, out)
		}
	}
	r := run(in, out, firstArgs...)
	logger.Infof("done in %+v with %+v", time.Since(start), r)
	return r
}
