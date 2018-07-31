package common

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/samwooo/bolsa/logging"
)

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
