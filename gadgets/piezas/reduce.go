package piezas

import (
	"fmt"
	"reflect"
	"time"
)

type reducer interface{} // a func that can do reducing
type _reduce struct{}

func (anonymous *_reduce) apply(r reducer, d, memo reflect.Value) reflect.Value {
	fn := reflect.Indirect(reflect.ValueOf(r))
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

func (anonymous *_reduce) doReduce(data array, memo interface{}, r reducer, done chan reflect.Value) {
	a := reflect.Indirect(reflect.ValueOf(data))
	kind := a.Kind()
	if kind != reflect.Array && kind != reflect.Slice {
		done <- anonymous.apply(r, reflect.ValueOf(data), reflect.ValueOf(memo))
	} else {
		m := reflect.ValueOf(memo)
		for i := 0; i < a.Len(); i++ {
			m = anonymous.apply(r, a.Index(i), m)
		}
		done <- m
	}
}

func Reduce(data array, memo interface{}, r reducer) interface{} {
	start := time.Now()
	done := make(chan reflect.Value)
	anonymous := _reduce{}
	go anonymous.doReduce(data, memo, r, done)
	result := <-done
	fmt.Println("<< reduce >> Done in ", time.Since(start))
	return result.Interface()
}
