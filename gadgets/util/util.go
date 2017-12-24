package util

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"time"

	"github.com/samwooo/bolsa/gadgets/logging"
)

const (
	APP_NAME = "bolsa"
)

func RandomDuration(maxSeconds int) time.Duration {
	return time.Duration(rand.Intn(maxSeconds)) * time.Second
}

func LogMetricsInfo(logger logging.Logger, action string, start time.Time) {
	logger.Info(logging.MetricsInfo{
		Action:   action,
		TimeCost: time.Since(start).Seconds(),
	})
}

func Validate(v interface{}) []error {
	var errors []error
	gatherError := func(fieldIndex int, supported bool) []error {
		var e error
		if supported {
			e = fmt.Errorf("wrong %s[%s]", reflect.TypeOf(v).Name(),
				reflect.TypeOf(v).Field(fieldIndex).Name)
		} else {
			e = fmt.Errorf("unimplemented type %d for %s", reflect.ValueOf(v).Field(fieldIndex).Kind(),
				strings.ToLower(reflect.TypeOf(v).Field(fieldIndex).Name))
		}
		errors = append(errors, e)
		return errors
	}
	for i := 0; i < reflect.TypeOf(v).NumField(); i++ {
		switch reflect.ValueOf(v).Field(i).Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			{
				if validate := reflect.ValueOf(v).Field(i).Int() > 0; !validate {
					gatherError(i, true)
				}
			}
		case reflect.String, reflect.Array, reflect.Slice, reflect.Map:
			{
				if validate := reflect.ValueOf(v).Field(i).Len() > 0; !validate {
					gatherError(i, true)
				}
			}
		case reflect.Struct:
			errors = append(errors, Validate(reflect.ValueOf(v).Field(i).Interface())...)
		default:
			gatherError(i, false)
		}
	}
	return errors
}

func Stringify(v interface{}) string {
	if v == nil {
		return "nil"
	} else {
		s := "\n" + reflect.TypeOf(v).Name()
		for i := 0; i < reflect.TypeOf(v).NumField(); i++ {
			switch reflect.ValueOf(v).Field(i).Kind() {
			case reflect.Struct, reflect.Interface:
				s += Stringify(reflect.ValueOf(v).Field(i).Interface())
			default:
				s += fmt.Sprintf("\n\t%s: %v", reflect.TypeOf(v).Field(i).Name,
					reflect.ValueOf(v).Field(i).Interface())
			}
		}
		return s
	}

}
