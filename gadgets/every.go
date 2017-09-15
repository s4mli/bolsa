package gadgets

import (
	"fmt"
	"reflect"
	"runtime"
	"time"
)

type tester interface{} // a func can do checking and then return a boolean
type _every struct{}

func (anonymous *_every) apply(t tester, d reflect.Value) bool {
	fn := reflect.Indirect(reflect.ValueOf(t))
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

func (anonymous *_every) feed(data array, in chan reflect.Value) {
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
}

func (anonymous *_every) digest(routines int, finished chan bool, out chan bool) {
	for i := 0; i < routines; i++ {
		<-finished
	}
	close(out)
}

func (anonymous *_every) chew(t tester, routines int, finished chan bool, in chan reflect.Value, out chan bool) {
	for i := 0; i < routines; i++ {
		go func(in chan reflect.Value, out chan bool, finished chan bool, t tester) {
			for data := range in {
				passed := anonymous.apply(t, data)
				out <- passed
				if !passed {
					break
				}
			}
			finished <- true
		}(in, out, finished, t)
	}
}

func (anonymous *_every) outlet(out chan bool, done chan bool) {
	result := true
	for passed := range out {
		result = result && passed
	}

	done <- result
}

func Every(data array, t tester) bool {
	start := time.Now()
	routines := runtime.NumCPU()
	in := make(chan reflect.Value, routines)
	out := make(chan bool, routines)
	finished := make(chan bool, routines)
	done := make(chan bool)
	anonymous := _every{}
	go anonymous.feed(data, in)
	go anonymous.chew(t, routines, finished, in, out)
	go anonymous.digest(routines, finished, out)
	go anonymous.outlet(out, done)
	r := <-done
	fmt.Println("<< every >> Done in ", time.Since(start))
	return r
}
