package job

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/job/feeder"
	"github.com/samwooo/bolsa/job/model"
	"github.com/samwooo/bolsa/logging"
	"github.com/stretchr/testify/assert"
)

var _ = logging.DefaultLogger("", logging.LogLevelFromString("ERROR"), 100)

type laborWithError struct{}

func (a *laborWithError) Work(p interface{}) (r interface{}, e error) {
	return nil, fmt.Errorf("test laborWithError")
}

type laborWithoutError struct{}

func (a *laborWithoutError) Work(p interface{}) (r interface{}, e error) {
	return p, nil
}

type JobTester struct {
	*Job
	maxRetry int
}

func (*JobTester) Worth(d model.Done) bool { return d.E != nil }
func (jt *JobTester) Limit() int           { return jt.maxRetry }
func (jt *JobTester) OnError(model.Done)   {}
func newJobTester(as model.LaborStrategy, with []interface{}, batch, maxRetry int, usingContext bool) *JobTester {
	var ctx = context.Background()
	var cancelFn context.CancelFunc = nil
	if usingContext {
		ctx, cancelFn = context.WithCancel(context.Background())
		time.AfterFunc(time.Millisecond*500, func() { cancelFn() })
	}
	jt := &JobTester{NewJob(logging.GetLogger(""), "", runtime.NumCPU(),
		feeder.NewDataFeeder(ctx, logging.GetLogger(""), with,
			batch, false)), maxRetry}
	jt.SetLaborStrategy(as).SetRetryStrategy(jt)
	if !usingContext {
		time.AfterFunc(time.Millisecond*500, func() { jt.Close() })
	}
	return jt
}

func TestJobWithoutFeeder(t *testing.T) {
	j := NewJob(logging.GetLogger(""), "", 0, nil)
	assert.Equal(t, (*Job)(nil), j)
}

func testJobWithNoLaborNoRetry(t *testing.T, flag bool) {
	with := []interface{}{1, 2, 3}
	jt := newJobTester(nil, with, 1, 0, flag)
	r := jt.Run()
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(model.Done)
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
func TestJobWithNoLaborNoRetryWithContext(t *testing.T) {
	testJobWithNoLaborNoRetry(t, true)
}

func TestJobWithNoLaborNoRetryWithDeadline(t *testing.T) {
	testJobWithNoLaborNoRetry(t, false)
}

func testJobWithNoLaborButRetry(t *testing.T, flag bool) {
	with := []interface{}{1, 2, 3}
	jt := newJobTester(nil, with, 1, 3, flag)
	r := jt.Run()
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(model.Done)
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

func TestJobWithNoLaborButRetryWithContext(t *testing.T) {
	testJobWithNoLaborButRetry(t, true)
}

func TestJobWithNoLaborButRetryWithDeadline(t *testing.T) {
	testJobWithNoLaborButRetry(t, false)
}

func testJobWithLaborErrorNoRetry(t *testing.T, flag bool) {
	with := []interface{}{1, 2, 3}
	jt := newJobTester(&laborWithError{}, with, 1, 0, flag)
	r := jt.Run()
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(model.Done)
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

func TestJobWithLaborErrorNoRetryWithContext(t *testing.T) {
	testJobWithLaborErrorNoRetry(t, true)
}

func TestJobWithLaborErrorNoRetryWithDeadline(t *testing.T) {
	testJobWithLaborErrorNoRetry(t, false)
}

func testJobWithLaborErrorButRetry(t *testing.T, flag bool) {
	with := []interface{}{1, 2, 3}
	jt := newJobTester(&laborWithError{}, with, 1, 3, flag)
	r := jt.Run()
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(model.Done)
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

func TestJobWithLaborErrorButRetryWithContext(t *testing.T) {
	testJobWithLaborErrorButRetry(t, true)
}

func TestJobWithLaborErrorButRetryWithDeadline(t *testing.T) {
	testJobWithLaborErrorButRetry(t, false)
}

func testJobWithLaborNoRetry(t *testing.T, flag bool) {
	with := []interface{}{1, 2, 3}
	jt := newJobTester(&laborWithoutError{}, with, 1, 0, flag)
	r := jt.Run()
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(model.Done)
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

func TestJobWithLaborNoRetryWithContext(t *testing.T) {
	testJobWithLaborNoRetry(t, true)
}

func TestJobWithLaborNoRetryWithDeadline(t *testing.T) {
	testJobWithLaborNoRetry(t, false)
}

func testJobWithLaborAndRetry(t *testing.T, flag bool) {
	with := []interface{}{1, 2, 3}
	jt := newJobTester(&laborWithoutError{}, with, 1, 3, flag)
	r := jt.Run()
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(model.Done)
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

func TestJobWithLaborAndRetryWithContext(t *testing.T) {
	testJobWithLaborAndRetry(t, true)
}

func TestJobWithLaborAndRetryWithDeadline(t *testing.T) {
	testJobWithLaborAndRetry(t, false)
}

func (jt *JobTester) Work(p interface{}) (r interface{}, e error) {
	v, _ := p.(int)
	if v%2 == 0 {
		return p, nil
	} else {
		return nil, fmt.Errorf("itself error")
	}
}

func testJobItselfWithRetry(t *testing.T, flag bool) {
	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	jt := newJobTester(nil, with, 1, 3, flag)
	r := jt.SetLaborStrategy(jt).SetRetryStrategy(jt).Run()
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(model.Done)
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

func TestJobItselfWithRetryWithContext(t *testing.T) {
	testJobItselfWithRetry(t, true)
}

func TestJobItselfWithRetryWithDeadline(t *testing.T) {
	testJobItselfWithRetry(t, false)
}

type retryHook struct{}

func (rh *retryHook) Worth(model.Done) bool { return false }
func (rh *retryHook) Limit() int            { return 3 }
func TestJobItselfWithNoRetry(t *testing.T) {
	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	jt := &JobTester{NewJob(logging.GetLogger(""), "", runtime.NumCPU(),
		feeder.NewDataFeeder(context.Background(), logging.GetLogger(""), with,
			1, false)), 3}
	jt.SetLaborStrategy(jt).SetRetryStrategy(&retryHook{})
	time.AfterFunc(time.Millisecond*500, func() { jt.Close() })
	r := jt.Run()
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(model.Done)
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

func (bj *blindlyRetryJob) Worth(model.Done) bool { return true }
func (bj *blindlyRetryJob) Limit() int            { return bj.maxRetry }
func (bj *blindlyRetryJob) Work(p interface{}) (r interface{}, e error) {
	return p, fmt.Errorf("|")
}

func testBlindlyRetryJob(t *testing.T, flag bool) {
	with := []interface{}{"blindlyRetryJob"}
	brj := &blindlyRetryJob{NewJob(logging.GetLogger(""), "", runtime.NumCPU(),
		feeder.NewDataFeeder(context.Background(), logging.GetLogger(""), with, 1,
			false)), 3}
	brj.SetLaborStrategy(brj).SetRetryStrategy(brj)
	time.AfterFunc(time.Millisecond*500, func() { brj.Close() })
	r := brj.Run()
	count := 0
	r.Range(func(key, value interface{}) bool {
		done, ok := value.(model.Done)
		assert.Equal(t, true, ok)
		assert.Equal(t, true, common.IsIn(done.D, with))
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, true, common.IsIn(done.R, with))
		assert.Equal(t, 3, done.Retries)
		count++
		return true
	})
	assert.Equal(t, len(with), count)
}

func TestBlindlyRetryJobWithContext(t *testing.T) {
	testBlindlyRetryJob(t, true)
}

func TestBlindlyRetryJobWithDeadline(t *testing.T) {
	testBlindlyRetryJob(t, false)
}

func testJobWithAdditionalPush(t *testing.T, batch int, data interface{}) {
	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9}
	jt := &JobTester{NewJob(logging.GetLogger(""), "", runtime.NumCPU(),
		feeder.NewDataFeeder(context.Background(), logging.GetLogger(""), with,
			batch, false)), 3}
	jt.SetLaborStrategy(&laborWithoutError{}).SetRetryStrategy(jt)

	var result *sync.Map
	ready := make(chan bool)
	go func() {

		result = jt.Run()
		ready <- true
	}()
	go func() {
		jt.Push(data)
		time.AfterFunc(time.Millisecond*500, func() { jt.Close() })
	}()

	dataArray, isArray := data.([]interface{})
	elements := with
	if isArray {
		elements = append(elements, dataArray...)
	} else {
		elements = append(elements, data)
	}

	var DS []interface{}
	if batch < 1 {
		DS = append(DS, with)
		if isArray {
			DS = append(DS, dataArray)
		} else {
			DS = append(DS, data)
		}
	} else if batch == 1 {
		DS = append(DS, with...)
		if isArray {
			DS = append(DS, dataArray...)
		} else {
			DS = append(DS, data)
		}
	} else {
		group := func(d []interface{}, batch int) (grouped []interface{}) {
			count := len(d)
			group := count / batch
			if count%batch > 0 {
				group += 1
			}
			for i := 0; i < group; i++ {
				start := i * batch
				end := i*batch + batch
				if end > count {
					end = count
				}
				grouped = append(grouped, d[start:end])
			}
			return
		}
		DS = append(DS, group(with, batch)...)
		if isArray {
			DS = append(DS, group(dataArray, batch)...)
		} else {
			DS = append(DS, data)
		}
	}

	<-ready
	count := 0
	result.Range(func(key, value interface{}) bool {
		done, ok := value.(model.Done)
		assert.Equal(t, true, ok)
		if dArray, isArray := done.D.([]interface{}); isArray {
			for _, d := range dArray {
				assert.Equal(t, true, common.IsIn(d, elements))
			}
		} else {
			assert.Equal(t, true, common.IsIn(done.D, elements))
		}

		if rArray, isArray := done.R.([]interface{}); isArray {
			for _, d := range rArray {
				assert.Equal(t, true, common.IsIn(d, elements))
			}
		} else {
			assert.Equal(t, true, common.IsIn(done.R, elements))
		}
		assert.Equal(t, nil, done.P)
		assert.Equal(t, nil, done.E)
		count++
		return true
	})
	assert.Equal(t, len(DS), count)
}

func testJobWithPushOneMoreWithBatch(t *testing.T, batch int) {
	testJobWithAdditionalPush(t, batch, 10)
}

func TestJobWithPushOneMoreBatchLessThan0(t *testing.T) {
	testJobWithAdditionalPush(t, -1, 10)
}
func TestJobWithPushOneMoreBatchEquals0(t *testing.T) {
	testJobWithAdditionalPush(t, 0, 10)
}
func TestJobWithPushOneMoreBatchEquals1(t *testing.T) {
	testJobWithAdditionalPush(t, 2, 10)
}
func TestJobWithPushOneMoreBatchBiggerThan1(t *testing.T) {
	testJobWithAdditionalPush(t, 2, 10)
}
func TestJobWithPushOneMoreBatchBiggerThanSize(t *testing.T) {
	testJobWithAdditionalPush(t, 100, 10)
}
func TestJobWithPushExistingOneBatchLessThan0(t *testing.T) {
	testJobWithAdditionalPush(t, -1, 2)
}
func TestJobWithPushExistingOneBatchEquals0(t *testing.T) {
	testJobWithAdditionalPush(t, 0, 2)
}
func TestJobWithPushExistingOneBatchEquals1(t *testing.T) {
	testJobWithAdditionalPush(t, 2, 2)
}
func TestJobWithPushExistingOneBatchBiggerThan1(t *testing.T) {
	testJobWithAdditionalPush(t, 2, 2)
}
func TestJobWithPushExistingOneBatchBiggerThanSize(t *testing.T) {
	testJobWithAdditionalPush(t, 100, 2)
}
func TestJobWithPushArrayBatchLessThan0(t *testing.T) {
	testJobWithAdditionalPush(t, -1, []interface{}{2})
}
func TestJobWithPushArrayBatchEquals0(t *testing.T) {
	testJobWithAdditionalPush(t, 0, []interface{}{2, 3, 4})
}
func TestJobWithPushArrayBatchEquals1(t *testing.T) {
	testJobWithAdditionalPush(t, 1, []interface{}{5, 6, 7})
}
func TestJobWithPushArrayBatchBiggerThan1(t *testing.T) {
	testJobWithAdditionalPush(t, 2, []interface{}{8, 9, 10})
}
func TestJobWithPushArrayBatchBiggerThanSize(t *testing.T) {
	testJobWithAdditionalPush(t, 100, []interface{}{100, 101, 121})
}
