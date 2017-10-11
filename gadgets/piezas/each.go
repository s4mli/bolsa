package piezas

import (
	"fmt"
	"reflect"
	"runtime"
	"time"
)

type each struct {
	routinesCount int
	routinesDone  chan bool
}

func (myself *each) apply(f iterator, d reflect.Value) {
	fn := reflect.Indirect(reflect.ValueOf(f))
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

func (myself *each) feed(data array) <-chan reflect.Value {
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

func (myself *each) digest() <-chan bool {
	done := make(chan bool)
	go func(myself *each, done chan<- bool) {
		for i := 0; i < myself.routinesCount; i++ {
			<-myself.routinesDone
		}
		done <- true
	}(myself, done)
	return done
}

func (myself *each) chew(in <-chan reflect.Value, f iterator) {
	for i := 0; i < myself.routinesCount; i++ {
		go func(myself *each, in <-chan reflect.Value, f iterator) {
			for data := range in {
				myself.apply(f, data)
			}
			myself.routinesDone <- true
		}(myself, in, f)
	}
}

func Each(data array, f iterator) {
	start := time.Now()
	routines := runtime.NumCPU()
	myself := each{
		routines,
		make(chan bool, routines)}

	myself.chew(myself.feed(data), f)
	<-myself.digest()
	fmt.Println("<< each >> Done in ", time.Since(start))
}
