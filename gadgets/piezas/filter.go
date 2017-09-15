package piezas

import (
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"time"
)

type filter interface{} // a func can do checking and then return a boolean
type _filter struct{}

func (anonymous *_filter) apply(f filter, d terco) bool {
	fn := reflect.Indirect(reflect.ValueOf(f))
	if fn.Kind() != reflect.Func {
		err := fmt.Errorf("<< filter >> Fliter must be a Func, skip")
		fmt.Println(err.Error())
		return false
	}
	fnt := fn.Type()
	if fnt.NumIn() != 1 {
		err := fmt.Errorf("<< filter >> Fliter should have 1 and only 1 para, skip")
		fmt.Println(err.Error())
		return false
	}
	if fnt.In(0) != d.Data.Type() {
		err := fmt.Errorf("<< filter >>  Undercover '%v', skip", d.Data.Interface())
		fmt.Println(err.Error())
		return false
	}
	if fnt.NumOut() != 1 {
		err := fmt.Errorf("<< filter >> Fliter should have 1 and only 1 return, skip")
		fmt.Println(err.Error())
		return false
	}
	if fnt.Out(0).Kind() != reflect.Bool {
		err := fmt.Errorf("<< filter >> Fliter should return bool, skip")
		fmt.Println(err.Error())
		return false
	}
	defer func(fn reflect.Value) {
		if r := recover(); r != nil {
			fmt.Println("<< filter >> Recover apply - ", fn.Type(), " -> ", r)
		}
	}(fn)
	return fn.Call([]reflect.Value{d.Data})[0].Interface().(bool)
}

func (anonymous *_filter) feed(data array, in chan terco) {
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
}

func (anonymous *_filter) digest(routines int, finished chan bool, out chan terco) {
	for i := 0; i < routines; i++ {
		<-finished
	}
	close(out)
}

func (anonymous *_filter) chew(f filter, routines int, finished chan bool, in chan terco, out chan terco) {
	for i := 0; i < routines; i++ {
		go func(in chan terco, out chan terco, finished chan bool, f filter) {
			for data := range in {
				passed := anonymous.apply(f, data)
				if passed {
					out <- data
				}
			}
			finished <- true
		}(in, out, finished, f)
	}
}

func (anonymous *_filter) outlet(stable bool, out chan terco, done chan []interface{}) {
	results := []interface{}{}
	if stable {
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
}

func Filter(data array, f filter, stable bool) []interface{} {
	start := time.Now()
	routines := runtime.NumCPU()
	in := make(chan terco, routines)
	out := make(chan terco, routines)
	finished := make(chan bool, routines)
	done := make(chan []interface{})
	anonymous := _filter{}
	go anonymous.feed(data, in)
	go anonymous.chew(f, routines, finished, in, out)
	go anonymous.digest(routines, finished, out)
	go anonymous.outlet(stable, out, done)
	r := <-done
	fmt.Println("<< filter >> Done in ", time.Since(start))
	return r
}
