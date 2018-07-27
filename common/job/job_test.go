package job

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/common/logging"
	"github.com/stretchr/testify/assert"
)

var _ = logging.DefaultLogger("", logging.LogLevelFromString("ERROR"), 100)

type laborWithError struct{}

func (a *laborWithError) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	return nil, fmt.Errorf("test laborWithError")
}

type laborWithoutError struct{}

func (a *laborWithoutError) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	return p, nil
}

type JobTester struct {
	*Job
	maxRetry int
}

func (*JobTester) Worth(d Done) bool { return d.E != nil }
func (jt *JobTester) Limit() int     { return jt.maxRetry }
func (jt *JobTester) OnError(Done)   {}
func newJobTester(as laborStrategy, with []interface{}, batch, maxRetry int) *JobTester {
	f := NewRetryableFeeder(context.Background(), with, batch, false)
	jt := &JobTester{NewJob(logging.GetLogger(""), "",
		runtime.NumCPU(), f), maxRetry}
	jt.LaborStrategy(as).RetryStrategy(jt).ErrorStrategy(jt)
	time.AfterFunc(time.Duration(time.Millisecond*time.Duration(len(with)*100)), func() { f.Close() })
	return jt
}

func TestJobWithNoLaborNoRetry(t *testing.T) {
	with := []interface{}{1, 2, 3}
	jt := newJobTester(nil, with, 1, 0)
	r := jt.Run(context.Background())
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(Done)
		assert.Equal(t, true, ok)
		assert.Equal(t, true, common.IsIn(done.D, with))
		assert.Equal(t, true, common.IsIn(done.R, with))
		assert.Equal(t, nil, done.P)
		assert.Equal(t, nil, done.E)
		count++
		return true
	})
	assert.Equal(t, len(with), count)
}

func TestJobWithNoLaborButRetry(t *testing.T) {
	with := []interface{}{1, 2, 3}
	jt := newJobTester(nil, with, 1, 3)
	r := jt.Run(context.Background())
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(Done)
		assert.Equal(t, true, ok)
		assert.Equal(t, true, common.IsIn(done.D, with))
		assert.Equal(t, true, common.IsIn(done.R, with))
		assert.Equal(t, nil, done.P)
		assert.Equal(t, nil, done.E)
		count++
		return true
	})
	assert.Equal(t, len(with), count)
}

func TestJobWithLaborErrorNoRetry(t *testing.T) {
	with := []interface{}{1, 2, 3}
	jt := newJobTester(&laborWithError{}, with, 1, 0)
	r := jt.Run(context.Background())
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(Done)
		assert.Equal(t, true, ok)
		assert.Equal(t, true, common.IsIn(done.D, with))
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, nil, done.R)
		assert.Equal(t, fmt.Sprintf("✗ labor failed ( %+v, test laborWithError )", done.P), done.E.Error())
		count++
		return true
	})
	assert.Equal(t, len(with), count)
}

func TestJobWithLaborErrorButRetry(t *testing.T) {
	with := []interface{}{1, 2, 3}
	jt := newJobTester(&laborWithError{}, with, 1, 3)
	r := jt.Run(context.Background())
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(Done)
		assert.Equal(t, true, ok)
		assert.Equal(t, true, common.IsIn(done.D, with))
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, nil, done.R)
		assert.Equal(t, fmt.Sprintf("✗ labor failed ( %+v, test laborWithError )", done.P), done.E.Error())
		count++
		return true
	})
	assert.Equal(t, len(with), count)
}

func TestJobWithLaborNoRetry(t *testing.T) {
	with := []interface{}{1, 2, 3}
	jt := newJobTester(&laborWithoutError{}, with, 1, 0)
	r := jt.Run(context.Background())
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(Done)
		assert.Equal(t, true, ok)
		assert.Equal(t, true, common.IsIn(done.D, with))
		assert.Equal(t, true, common.IsIn(done.R, with))
		assert.Equal(t, nil, done.P)
		assert.Equal(t, nil, done.E)
		count++
		return true
	})
	assert.Equal(t, len(with), count)
}

func TestJobWithLaborAndRetry(t *testing.T) {
	with := []interface{}{1, 2, 3}
	jt := newJobTester(&laborWithoutError{}, with, 1, 3)
	r := jt.Run(context.Background())
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(Done)
		assert.Equal(t, true, ok)
		assert.Equal(t, true, common.IsIn(done.D, with))
		assert.Equal(t, true, common.IsIn(done.R, with))
		assert.Equal(t, nil, done.P)
		assert.Equal(t, nil, done.E)
		count++
		return true
	})
	assert.Equal(t, len(with), count)
}

func (jt *JobTester) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	v, _ := p.(int)
	if v%2 == 0 {
		return p, nil
	} else {
		return nil, fmt.Errorf("itself error")
	}
}

func TestJobItselfWithRetry(t *testing.T) {
	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	f := NewRetryableFeeder(context.Background(), with, 1, false)
	jt := &JobTester{NewJob(logging.GetLogger(""), "", runtime.NumCPU(), f), 3}
	jt.LaborStrategy(jt).RetryStrategy(jt).ErrorStrategy(jt)
	time.AfterFunc(time.Duration(time.Millisecond*time.Duration(len(with)*100)), func() { f.Close() })
	r := jt.Run(context.Background())
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(Done)
		assert.Equal(t, true, ok)
		assert.Equal(t, true, common.IsIn(done.D, with))
		if done.E != nil {
			v, _ := done.P.(int)
			assert.Equal(t, 1, v%2)
			assert.Equal(t, nil, done.R)
			assert.Equal(t, true, common.IsIn(done.P, with))
			assert.Equal(t, fmt.Sprintf("✗ labor failed ( %v, itself error )", done.P), done.E.Error())
		} else {
			v, _ := done.R.(int)
			assert.Equal(t, 0, v%2)
			assert.Equal(t, nil, done.P)
			assert.Equal(t, true, common.IsIn(done.R, with))
		}
		count++
		return true
	})
	assert.Equal(t, len(with), count)
}

type retryHook struct{}

func (rh *retryHook) Worth(Done) bool { return false }
func (rh *retryHook) Limit() int      { return 3 }
func TestJobItselfWithNoRetry(t *testing.T) {
	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	f := NewRetryableFeeder(context.Background(), with, 1, false)
	jt := &JobTester{NewJob(logging.GetLogger(""), "", runtime.NumCPU(), f), 3}
	jt.LaborStrategy(jt).RetryStrategy(&retryHook{}).ErrorStrategy(jt)
	time.AfterFunc(time.Duration(time.Millisecond*time.Duration(len(with)*100)), func() { f.Close() })
	r := jt.Run(context.Background())
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(Done)
		assert.Equal(t, true, ok)
		assert.Equal(t, true, common.IsIn(done.D, with))
		if done.E != nil {
			v, _ := done.P.(int)
			assert.Equal(t, 1, v%2)
			assert.Equal(t, nil, done.R)
			assert.Equal(t, true, common.IsIn(done.P, with))
			assert.Equal(t, fmt.Sprintf("✗ labor failed ( %v, itself error )", done.P), done.E.Error())
		} else {
			v, _ := done.R.(int)
			assert.Equal(t, 0, v%2)
			assert.Equal(t, nil, done.P)
			assert.Equal(t, true, common.IsIn(done.R, with))
		}
		count++
		return true
	})
	assert.Equal(t, len(with), count)
}

type blindlyRetryJob struct {
	*Job
	maxRetry int
}

func (bj *blindlyRetryJob) Worth(Done) bool { return true }
func (bj *blindlyRetryJob) Limit() int      { return bj.maxRetry }
func (bj *blindlyRetryJob) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	return p, fmt.Errorf("|")
}

func TestBlindlyRetryJob(t *testing.T) {
	with := []interface{}{"blindlyRetryJob"}
	f := NewRetryableFeeder(context.Background(), with, 1, false)
	brj := &blindlyRetryJob{NewJob(logging.GetLogger(""), "", runtime.NumCPU(),
		f), 3}
	brj.LaborStrategy(brj).RetryStrategy(brj)
	time.AfterFunc(time.Duration(time.Millisecond*time.Duration(len(with)*300)), func() { f.Close() })
	r := brj.Run(context.Background())
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(Done)
		assert.Equal(t, true, ok)
		assert.Equal(t, true, common.IsIn(done.D, with))
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, true, common.IsIn(done.R, with))
		assert.Equal(t, 3, done.retries)
		count++
		return true
	})
	assert.Equal(t, len(with), count)
}
