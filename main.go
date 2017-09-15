package main

import (
	"bolsa/gadgets"
	"fmt"
	"strconv"
)

func main() {
	if true { // waterfall
		gadgets.NewWaterfall(gadgets.Tasks{
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
		r := gadgets.Map([]int64{1, 2, 3, 4, 5, 6, 7, 8, 99, 1112, 213, 123, 542, 45, 56, 345,
			623, 5, 12, 3123, 123334, 23412, 21341234, 1234, 123, 412, 34, 123, 413, 45, 324,
			52, 4523, 45, 134, 523, 4312, 34, 1234, 12, 341, 234, 123, 1234, 1345, 34, 564, 325,
			5623463, 67324523, 423452345, 323452345, 22345234, 2345678963},
			func(k int64) int64 {
				return k * k
			}, true)
		fmt.Println(r)
	}
	if true { // map
		r := gadgets.Map("8",
			func(k string) int {
				if r, err := strconv.Atoi(k); err != nil {
					return 0
				} else {
					return r * r
				}
			}, true)
		fmt.Println(r)
	} // every
	if true {
		r := gadgets.Every(
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 99, 1112, 213, 123, 542, 45, 56, 345, 623},
			func(k int64) bool { return k < 0 })
		fmt.Println(r)
	}
}
