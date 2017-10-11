package piezas

import (
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"time"
)

type _map struct {
	stable        bool
	routinesCount int
	routinesDone  chan bool
}

func (myself *_map) apply(f iterator, d terco) terco {
	fn := reflect.Indirect(reflect.ValueOf(f))
	if fn.Kind() != reflect.Func {
		err := fmt.Errorf("<< map >> Mapper must be a Func, skip")
		fmt.Println(err.Error())
		return d
	}
	fnt := fn.Type()
	if fnt.NumIn() != 1 {
		err := fmt.Errorf("<< map >> Mapper should have 1 and only 1 para, skip")
		fmt.Println(err.Error())
		return d
	}
	if fnt.In(0) != d.Data.Type() {
		err := fmt.Errorf("<< map >>  Undercover '%v', skip", d.Data.Interface())
		fmt.Println(err.Error())
		return d
	}

	defer func(fn reflect.Value) {
		if r := recover(); r != nil {
			fmt.Println("<< map >> Recover apply - ", fn.Type(), " -> ", r)
		}
	}(fn)

	return terco{fn.Call([]reflect.Value{d.Data})[0], d.Index}
}

func (myself *_map) feed(data array) <-chan terco {
	in := make(chan terco, myself.routinesCount)
	go func(data array, in chan<- terco) {
		a := reflect.Indirect(reflect.ValueOf(data))
		kind := a.Kind()
		if kind != reflect.Array && kind != reflect.Slice {
			in <- terco{reflect.ValueOf(data), 0}
		} else {
			for i := 0; i < a.Len(); i++ {
				in <- terco{a.Index(i), i}
			}
		}
		close(in)
	}(data, in)
	return in
}

func (myself *_map) digest(out <-chan terco) []interface{} {
	done := make(chan []interface{})
	go func(myself *_map, out <-chan terco, done chan<- []interface{}) {
		results := []interface{}{}
		if myself.stable {
			s := tercos{}
			for d := range out {
				s = append(s, d)
			}
			sort.Sort(s)
			for _, d := range s {
				results = append(results, d.Data.Interface())
			}
		} else {
			for d := range out {
				results = append(results, d.Data.Interface())
			}
		}
		done <- results
	}(myself, out, done)
	return <-done
}

func (myself *_map) chew(in <-chan terco, f iterator) <-chan terco {
	out := make(chan terco, myself.routinesCount)
	for i := 0; i < myself.routinesCount; i++ {
		go func(myself *_map, in <-chan terco, out chan<- terco, f iterator) {
			for data := range in {
				out <- myself.apply(f, data)
			}
			myself.routinesDone <- true
		}(myself, in, out, f)
	}

	go func(myself *_map, out chan<- terco) {
		for i := 0; i < myself.routinesCount; i++ {
			<-myself.routinesDone
		}
		close(out)
	}(myself, out)
	return out
}

func Map(data array, f iterator, stable bool) []interface{} {
	start := time.Now()
	routines := runtime.NumCPU()
	myself := _map{
		stable,
		routines,
		make(chan bool, routines)}

	r := myself.digest(myself.chew(myself.feed(data), f))
	fmt.Println("<< map >> Done in ", time.Since(start))
	return r
}
