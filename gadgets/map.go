package gadgets

import (
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"time"
)

type mapper interface{} // a func can do mapping
type _map struct{}

func (anonymous *_map) apply(m mapper, d terco) terco {
	fn := reflect.Indirect(reflect.ValueOf(m))
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

func (anonymous *_map) feed(data array, in chan terco) {
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

func (anonymous *_map) digest(routines int, finished chan bool, out chan terco) {
	for i := 0; i < routines; i++ {
		<-finished
	}
	close(out)
}

func (anonymous *_map) chew(m mapper, routines int, finished chan bool, in chan terco, out chan terco) {
	for i := 0; i < routines; i++ {
		go func(in chan terco, out chan terco, finished chan bool, m mapper) {
			for data := range in {
				out <- anonymous.apply(m, data)
			}
			finished <- true
		}(in, out, finished, m)
	}
}

func (anonymous *_map) outlet(stable bool, out chan terco, done chan []interface{}) {
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

func Map(data array, m mapper, stable bool) []interface{} {
	start := time.Now()
	routines := runtime.NumCPU()
	in := make(chan terco, routines)
	out := make(chan terco, routines)
	finished := make(chan bool, routines)
	done := make(chan []interface{})
	anonymous := _map{}
	go anonymous.feed(data, in)
	go anonymous.chew(m, routines, finished, in, out)
	go anonymous.digest(routines, finished, out)
	go anonymous.outlet(stable, out, done)
	r := <-done
	fmt.Println("<< map >> Done in ", time.Since(start))
	return r
}
