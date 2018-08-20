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
)

type onCancel func()
type onSignal func(os.Signal)

func TerminateIf(ctx context.Context, onCancel onCancel, onSignal onSignal) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGILL, syscall.SIGTERM,
		syscall.SIGTRAP, syscall.SIGQUIT, syscall.SIGABRT)
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

func ErrorToString(errors []error) string {
	var errStr []string
	for _, err := range errors {
		errStr = append(errStr, err.Error())
	}
	return strings.Join(errStr, " | ")
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
