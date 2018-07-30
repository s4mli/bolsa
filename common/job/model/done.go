package model

import (
	"fmt"
	"time"
)

////////////////////////
// Task & Job Result //
type Done struct {
	P       interface{} // parameter
	R       interface{} // result
	E       error       // error
	D       interface{} // original data
	Key     string      // key
	Retries int         // retry times
}

func (d *Done) String() string {
	return fmt.Sprintf("\n ⬨ D: %+v\n ⬨ P: %+v\n ⬨ R , E: ( %+v , %+v )\n ⬨ Key: %s\n ⬨ Retries: %d",
		d.D, d.P, d.R, d.E, d.Key, d.Retries)
}
func NewDone(para, result interface{}, err error, retries int, d interface{}, k string) Done {
	return Done{para, result, err, d, k, retries}
}
func KeyFrom(d interface{}) string { return fmt.Sprintf("%+v-%d", d, time.Now().UnixNano()) }
