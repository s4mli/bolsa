package piezas

import (
	"fmt"
	"reflect"
	"time"
)

type task interface{} // a func that runs in waterfall
type step struct {
	Task task
}

func (s *step) process(args []reflect.Value) []reflect.Value {
	fn := reflect.Indirect(reflect.ValueOf(s.Task))
	if fn.Kind() != reflect.Func {
		err := fmt.Errorf("<< waterfall >> Task must be a Func")
		fmt.Println(err.Error())
		return []reflect.Value{reflect.ValueOf(err)}
	}

	defer func(fn reflect.Value) {
		fnt := fn.Type()
		//fnt.NumIn() != len(args)
		if r := recover(); r != nil {
			fmt.Println("<< waterfall >> Recover - ", fnt, " ", r)
		}
	}(fn)
	return fn.Call(args)
}

func (s *step) run(in <-chan []reflect.Value) <-chan []reflect.Value {
	out := make(chan []reflect.Value)
	go func(s *step, in <-chan []reflect.Value, out chan<- []reflect.Value) {
		for data := range in {
			out <- s.process(data)
		}
		close(out)
	}(s, in, out)
	return out
}

func newStep(t task) *step {
	return &step{t}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Tasks []task
type Waterfall struct{}

func (wf *Waterfall) run(in chan<- []reflect.Value, out <-chan []reflect.Value,
	firstArgs ...interface{}) []interface{} {

	go func(in chan<- []reflect.Value, firstArgs ...interface{}) {
		args := []reflect.Value{}
		for _, arg := range firstArgs {
			args = append(args, reflect.ValueOf(arg))
		}
		in <- args
		close(in)
	}(in, firstArgs...)

	results := []interface{}{}
	for _, v := range <-out {
		results = append(results, v.Interface())
	}
	return results
}

func NewWaterfall(tasks Tasks, firstArgs ...interface{}) []interface{} {
	start := time.Now()
	in := make(chan []reflect.Value)
	var out <-chan []reflect.Value
	for _, task := range tasks {
		s := newStep(task)
		if out == nil {
			out = s.run(in)
		} else {
			out = s.run(out)
		}
	}

	wf := Waterfall{}
	r := wf.run(in, out, firstArgs...)
	fmt.Println("<< waterfall >> Done in ", time.Since(start))
	return r
}
