package common

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/samwooo/bolsa/logging"
)

type onCancel func()
type onSignal func(os.Signal)

func TerminateIf(ctx context.Context, onCancel onCancel, onSignal onSignal) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGILL, syscall.SIGSYS,
		syscall.SIGTERM, syscall.SIGTRAP, syscall.SIGQUIT, syscall.SIGABRT)
	go func() {
		for {
			select {
			case <-ctx.Done():
				onCancel()
				return
			case s := <-sig:
				onSignal(s)
				return
			default:
				time.Sleep(time.Millisecond * 10)
			}
		}
	}()
}

func ErrorFromString(s string) error {
	if s == "" {
		return nil
	} else {
		return fmt.Errorf(s)
	}
}

func IsIn(k interface{}, arr []interface{}) bool {
	for _, v := range arr {
		if v == k {
			return true
		}
	}
	return false
}

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

// find elements that in slice1 but not in slice2
func SliceDiff(slice1 []interface{}, slice2 []interface{}) []interface{} {
	diff := []interface{}{}
	for _, s1 := range slice1 {
		found := false
		for _, s2 := range slice2 {
			if reflect.TypeOf(s1).Name() == reflect.TypeOf(s2).Name() &&
				reflect.ValueOf(s1).Interface() == reflect.ValueOf(s2).Interface() {
				found = true
				break
			}
		}
		if !found {
			diff = append(diff, s1)
		}
	}
	return diff
}
