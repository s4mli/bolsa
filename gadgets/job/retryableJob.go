package job

import (
	"context"

	"fmt"

	"math/rand"

	"strconv"

	"strings"

	"github.com/samwooo/bolsa/gadgets/logging"
)

// a job with retry, also indicate how to use job
type RetryableJob struct {
	maxRetry int
	curRetry int
	*Job
}

func (*RetryableJob) batchSize() int {
	return 8
}

func (*RetryableJob) doBatch(ctx context.Context, groupedMash []interface{}) (interface{}, error) {
	mashStr := "|"
	for _, m := range groupedMash {
		k, _ := m.(int)
		mashStr += strconv.Itoa(k) + "|"
	}
	return strings.Repeat(mashStr, 22), nil
}

func (*RetryableJob) doAction(ctx context.Context, p interface{}) (r interface{}, e error) {
	if bodyStr, ok := p.(string); ok {
		return " @ " + bodyStr + " @ ", nil
	} else {
		return nil, fmt.Errorf("cast body failed")
	}
}

func (rj *RetryableJob) worthRetry(Done) bool {
	return rand.Int()%rj.maxRetry > rj.curRetry
}

func (rj *RetryableJob) forgoRetry() bool {
	ended := rj.curRetry >= rj.maxRetry
	rj.curRetry++
	return ended
}

func NewRetryableJob(logger logging.Logger) *RetryableJob {
	rj := &RetryableJob{3, 0, NewJob(logger, 0)}
	rj.BatchWanted(rj).ActionWanted(rj).RetryWanted(rj)
	return rj
}
