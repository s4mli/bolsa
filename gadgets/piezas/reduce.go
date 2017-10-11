package piezas

import (
	"fmt"
	"reflect"
	"time"
)

type reduce struct{}

func (myself *reduce) apply(f iterator, d, memo reflect.Value) reflect.Value {
	fn := reflect.Indirect(reflect.ValueOf(f))
	if fn.Kind() != reflect.Func {
		err := fmt.Errorf("<< reduce >> Reducer must be a Func, skip")
		fmt.Println(err.Error())
		return memo
	}
	fnt := fn.Type()
	if fnt.NumIn() != 2 {
		err := fmt.Errorf("<< reduce >> Reducer should have 2 and only 2 paras, skip")
		fmt.Println(err.Error())
		return memo
	}
	if fnt.In(0) != d.Type() {
		err := fmt.Errorf("<< reduce >> Undercover '%v', skip", d.Interface())
		fmt.Println(err.Error())
		return memo
	}
	if fnt.In(1) != memo.Type() {
		err := fmt.Errorf("<< reduce >> Type mismatch '%v', skip", d.Interface())
		fmt.Println(err.Error())
		return memo
	}
	if fnt.NumOut() != 1 {
		err := fmt.Errorf("<< reduce >> Reducer should have 1 and only 1 return, skip")
		fmt.Println(err.Error())
		return memo
	}
	defer func(fn reflect.Value) {
		if r := recover(); r != nil {
			fmt.Println("<< reduce >> Recover apply - ", fn.Type(), " -> ", r)
		}
	}(fn)
	return fn.Call([]reflect.Value{d, memo})[0]
}

func (myself *reduce) reduce(data array, memo interface{}, f iterator) interface{} {
	done := make(chan reflect.Value)
	go func(myself *reduce, data array, memo interface{}, f iterator, done chan<- reflect.Value) {
		a := reflect.Indirect(reflect.ValueOf(data))
		kind := a.Kind()
		if kind != reflect.Array && kind != reflect.Slice {
			done <- myself.apply(f, reflect.ValueOf(data), reflect.ValueOf(memo))
		} else {
			m := reflect.ValueOf(memo)
			for i := 0; i < a.Len(); i++ {
				m = myself.apply(f, a.Index(i), m)
			}
			done <- m
		}
	}(myself, data, memo, f, done)
	return (<-done).Interface()
}

func Reduce(data array, memo interface{}, f iterator) interface{} {
	start := time.Now()
	myself := reduce{}
	r := myself.reduce(data, memo, f)
	fmt.Println("<< reduce >> Done in ", time.Since(start))
	return r
}
