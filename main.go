package main

import (
	"bolsa/gadgets/piezas"
	"fmt"
	"strconv"
	"sync"
)

func main() {
	if true { // waterfall
		piezas.NewWaterfall(piezas.Tasks{
			func(a ...int) (int, int, error) {
				total := 1
				count := 0
				for _, arg := range a {
					total = total * arg
					count = count + arg
				}
				return total, count, nil
			},
			func(a int, b int, e error) int {
				return a + b
			},
			func(a int) {
				fmt.Println("waterfall result :", a)
			}}, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	}
	if true { // map slice
		r := piezas.Map([]int64{1, 2, 3, 4, 5, 6, 7, 8, 99, 1112, 213, 123, 542, 45, 56, 345,
			623, 5, 12, 3123, 123334, 23412, 21341234, 1234, 123, 412, 34, 123, 413, 45, 324,
			52, 4523, 45, 134, 523, 4312, 34, 1234, 12, 341, 234, 123, 1234, 1345, 34, 564, 325,
			5623463, 67324523, 423452345, 323452345, 22345234, 2345678963},
			func(k int64) int64 {
				return k * k
			}, true)
		fmt.Println(r)
	}
	if true { // map
		r := piezas.Map("8",
			func(k string) int {
				if r, err := strconv.Atoi(k); err != nil {
					return 0
				} else {
					return r * r
				}
			}, true)
		fmt.Println(r)
	}
	if true { // every
		r := piezas.Every(
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 99, 1112, 213, 123, 542, 45, 56, 345, 623},
			func(k int64) bool { return k < 0 })
		fmt.Println(r)
	}
	if true { // each
		callCount := 0
		mutex := sync.Mutex{}
		input := []int64{1, 2, 3, 4, 5, 6, 7, 8, 99, 1112, 213, 123, 542, 45, 56, 345, 623}
		piezas.Each(input,
			func(k int64) {
				mutex.Lock()
				callCount++
				mutex.Unlock()
			})
		fmt.Println(callCount == len(input))
	}
	if true { // reduce
		input := []int64{1, 2, 3, 4, 5, 6, 7, 8, 99, 1112, 213, 123, 542, 45, 56, 345, 623}
		r := piezas.Reduce(input, 0,
			func(k int64, memo int) int {
				return memo + int(k)
			})
		fmt.Println(r)
	}
}
