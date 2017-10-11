package piezas

import "reflect"

type array interface{}
type iterator interface{}

type terco struct {
	Data  reflect.Value
	Index int
}

type tercos []terco

func (s tercos) Len() int           { return len(s) }
func (s tercos) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s tercos) Less(i, j int) bool { return s[i].Index < s[j].Index }
