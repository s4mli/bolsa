package piezas

import (
	"fmt"
	"reflect"
	"runtime"
	"time"
)

type every struct {
	routinesCount int
	routinesDone  chan bool
}

func (myself *every) apply(f iterator, d reflect.Value) bool {
	fn := reflect.Indirect(reflect.ValueOf(f))
	if fn.Kind() != reflect.Func {
		err := fmt.Errorf("<< every >> Tester must be a Func, skip")
		fmt.Println(err.Error())
		return false
	}
	fnt := fn.Type()
	if fnt.NumIn() != 1 {
		err := fmt.Errorf("<< every >> Tester should have 1 and only 1 para, skip")
		fmt.Println(err.Error())
		return false
	}
	if fnt.In(0) != d.Type() {
		err := fmt.Errorf("<< every >>  Undercover '%v', skip", d.Interface())
		fmt.Println(err.Error())
		return false
	}
	if fnt.NumOut() != 1 {
		err := fmt.Errorf("<< every >> Tester should have 1 and only 1 return, skip")
		fmt.Println(err.Error())
		return false
	}
	if fnt.Out(0).Kind() != reflect.Bool {
		err := fmt.Errorf("<< every >> Tester should return bool, skip")
		fmt.Println(err.Error())
		return false
	}
	defer func(fn reflect.Value) {
		if r := recover(); r != nil {
			fmt.Println("<< every >> Recover apply - ", fn.Type(), " -> ", r)
		}
	}(fn)
	return fn.Call([]reflect.Value{d})[0].Interface().(bool)
}

func (myself *every) feed(data array) <-chan reflect.Value {
	in := make(chan reflect.Value, myself.routinesCount)
	go func(data array, in chan<- reflect.Value) {
		a := reflect.Indirect(reflect.ValueOf(data))
		kind := a.Kind()
		if kind != reflect.Array && kind != reflect.Slice {
			in <- reflect.ValueOf(data)
		} else {
			for i := 0; i < a.Len(); i++ {
				in <- a.Index(i)
			}
		}
		close(in)
	}(data, in)
	return in
}

func (myself *every) digest(out <-chan bool) bool {
	done := make(chan bool)
	go func(myself *every, out <-chan bool, done chan<- bool) {
		result := true
		for passed := range out {
			result = result && passed
		}
		done <- result
	}(myself, out, done)
	return <-done
}

func (myself *every) chew(in <-chan reflect.Value, f iterator) <-chan bool {
	out := make(chan bool, myself.routinesCount)
	for i := 0; i < myself.routinesCount; i++ {
		go func(myself *every, in <-chan reflect.Value, out chan<- bool, f iterator) {
			for data := range in {
				passed := myself.apply(f, data)
				out <- passed
				if !passed {
					break
				}
			}
			myself.routinesDone <- true
		}(myself, in, out, f)
	}

	go func(myself *every, out chan<- bool) {
		for i := 0; i < myself.routinesCount; i++ {
			<-myself.routinesDone
		}
		close(out)
	}(myself, out)
	return out
}

func Every(data array, f iterator) bool {
	start := time.Now()
	routines := runtime.NumCPU()
	myself := every{
		routines,
		make(chan bool, routines)}

	r := myself.digest(myself.chew(myself.feed(data), f))
	fmt.Println("<< every >> Done in ", time.Since(start))
	return r
}
