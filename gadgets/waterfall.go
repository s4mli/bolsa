package gadgets

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

func (s *step) run(in chan []reflect.Value) chan []reflect.Value {
	out := make(chan []reflect.Value)
	go func(s *step) {
		for data := range in {
			out <- s.process(data)
		}
		close(out)
	}(s)
	return out
}

func newStep(t task) *step {
	return &step{t}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Tasks []task

type Waterfall struct {
	In  chan []reflect.Value
	Out chan []reflect.Value
}

func (wf *Waterfall) feed(in chan []reflect.Value, firstArgs ...interface{}) {
	go func(in chan []reflect.Value, firstArgs ...interface{}) {
		args := []reflect.Value{}
		for _, arg := range firstArgs {
			args = append(args, reflect.ValueOf(arg))
		}
		in <- args
		close(in)
	}(wf.In, firstArgs...)
}

func (wf *Waterfall) done(start time.Time) {
	for range wf.Out {
	}
	fmt.Println("<< waterfall >> Done in ", time.Since(start))
}

func NewWaterfall(tasks Tasks, firstArgs ...interface{}) {
	start := time.Now()
	in := make(chan []reflect.Value)
	var out chan []reflect.Value
	for _, task := range tasks {
		s := newStep(task)
		if out == nil {
			out = s.run(in)
		} else {
			out = s.run(out)
		}
	}

	waterfall := &Waterfall{in, out}
	waterfall.feed(in, firstArgs...)
	waterfall.done(start)
}
