package piezas

import (
	"fmt"
	"reflect"
	"runtime"
	"time"
)

type visitor interface{} // a func that visit each items
type _each struct{}

func (anonymous *_each) apply(v visitor, d reflect.Value) {
	fn := reflect.Indirect(reflect.ValueOf(v))
	if fn.Kind() != reflect.Func {
		err := fmt.Errorf("<< each >> Visitor must be a Func, skip")
		fmt.Println(err.Error())
		return
	}
	fnt := fn.Type()
	if fnt.NumIn() != 1 {
		err := fmt.Errorf("<< each >> Visitor should have 1 and only 1 para, skip")
		fmt.Println(err.Error())
		return
	}
	if fnt.In(0) != d.Type() {
		err := fmt.Errorf("<< each >>  Undercover '%v', skip", d.Interface())
		fmt.Println(err.Error())
		return
	}
	defer func(fn reflect.Value) {
		if r := recover(); r != nil {
			fmt.Println("<< each >> Recover apply - ", fn.Type(), " -> ", r)
		}
	}(fn)
	fn.Call([]reflect.Value{d})
}

func (anonymous *_each) feed(data array, in chan reflect.Value) {
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

func (anonymous *_each) digest(routines int, finished chan bool, done chan bool) {
	for i := 0; i < routines; i++ {
		<-finished
	}
	done <- true
}

func (anonymous *_each) chew(v visitor, routines int, finished chan bool, in chan reflect.Value) {
	for i := 0; i < routines; i++ {
		go func(in chan reflect.Value, finished chan bool, v visitor) {
			for data := range in {
				anonymous.apply(v, data)
			}
			finished <- true
		}(in, finished, v)
	}
}

func Each(data array, v visitor) {
	start := time.Now()
	routines := runtime.NumCPU()
	in := make(chan reflect.Value, routines)
	finished := make(chan bool, routines)
	done := make(chan bool)
	anonymous := _each{}
	go anonymous.feed(data, in)
	go anonymous.chew(v, routines, finished, in)
	go anonymous.digest(routines, finished, done)
	<-done
	fmt.Println("<< each >> Done in ", time.Since(start))
}
