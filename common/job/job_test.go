package job

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/common/logging"
	"github.com/stretchr/testify/assert"
)

const (
	WHAT = "job test "
)

type batch1WithError struct{}

func (b *batch1WithError) Size() int { return 1 }
func (b *batch1WithError) Reduce(context.Context, []interface{}) (interface{}, error) {
	return nil, fmt.Errorf("test batch1WithError")
}

type batch1WithoutError struct{}

func (b *batch1WithoutError) Size() int { return 1 }
func (b *batch1WithoutError) Reduce(ctx context.Context, groupedMash []interface{}) (interface{}, error) {
	return groupedMash[0], nil
}

type batchNWithError struct{ n int }

func (b *batchNWithError) Size() int { return b.n }
func (b *batchNWithError) Reduce(context.Context, []interface{}) (interface{}, error) {
	return nil, fmt.Errorf("test batch%dWithError", b.n)
}

type batchNWithoutError struct{ n int }

func (b *batchNWithoutError) Size() int { return b.n }
func (b *batchNWithoutError) Reduce(ctx context.Context, groupedMash []interface{}) (interface{}, error) {
	return groupedMash[len(groupedMash)-1], nil
}

type laborWithError struct{}

func (a *laborWithError) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	return nil, fmt.Errorf("test laborWithError")
}

type laborWithoutError struct{}

func (a *laborWithoutError) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	return p, nil
}

// Job Tester
type JobTester struct {
	*Job
	maxRetry, curRetry int
}

func (*JobTester) Worth(d Done) bool { return d.E != nil }

func (jt *JobTester) Forgo() bool {
	jt.curRetry++
	return jt.curRetry >= jt.maxRetry
}

func (jt *JobTester) OnError(Done) {}

func newJobTester(bs batchStrategy, as laborStrategy) *JobTester {
	logging.DefaultLogger(fmt.Sprintf(" < %s > ", common.APP_NAME),
		logging.LogLevelFromString("INFO"), 100)

	jt := &JobTester{NewJob(logging.GetLogger(WHAT), runtime.NumCPU()), 3, 0}
	jt.BatchStrategy(bs).LaborStrategy(as).RetryStrategy(jt).ErrorStrategy(jt)
	return jt
}

// nil nil
func TestJobWithNoBatchNoLabor(t *testing.T) {
	jt := newJobTester(nil, nil)
	with := []interface{}{1, 2, 3}
	assert.Equal(t, 1, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, len(with), len(allDone))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, true, common.IsIn(done.R, with))
		assert.Equal(t, nil, done.E)
	}
}

// nil withError
func TestJobWithNoBatchButLaborError(t *testing.T) {
	jt := newJobTester(nil, &laborWithError{})
	with := []interface{}{1, 2, 3}
	assert.Equal(t, 1, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, len(with), len(allDone))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, nil, done.R)
		assert.Equal(t, fmt.Sprintf("× labor failed ( %+v, test laborWithError )", done.P), done.E.Error())
	}
}

// nil withoutError
func TestJobWithNoBatchButLabor(t *testing.T) {
	jt := newJobTester(nil, &laborWithoutError{})
	with := []interface{}{1, 2, 3}
	assert.Equal(t, 1, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, len(with), len(allDone))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, true, common.IsIn(done.R, with))
		assert.Equal(t, nil, done.E)
	}
}

// 1-withError nil
func TestJobBatch1WithErrorNoLabor(t *testing.T) {
	jt := newJobTester(&batch1WithError{}, nil)
	with := []interface{}{1, 2, 3}
	assert.Equal(t, 1, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, len(with), len(allDone))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, nil, done.R)
		assert.Equal(t,
			fmt.Sprintf("× reduce failed ( %+v, test batch1WithError )", []interface{}{done.P}),
			done.E.Error())
	}
}

// 1-withError withError
func TestJobBatch1WithErrorLaborWithError(t *testing.T) {
	jt := newJobTester(&batch1WithError{}, &laborWithError{})
	with := []interface{}{1, 2, 3}
	assert.Equal(t, 1, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, len(with), len(allDone))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, nil, done.R)
		assert.Equal(t,
			fmt.Sprintf("× reduce failed ( %+v, test batch1WithError )", []interface{}{done.P}),
			done.E.Error())
	}
}

// 1-withError withoutError
func TestJobBatch1WithErrorLaborWithoutError(t *testing.T) {
	jt := newJobTester(&batch1WithError{}, &laborWithoutError{})
	with := []interface{}{1, 2, 3}
	assert.Equal(t, 1, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, len(with), len(allDone))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, nil, done.R)
		assert.Equal(t,
			fmt.Sprintf("× reduce failed ( %+v, test batch1WithError )", []interface{}{done.P}),
			done.E.Error())
	}
}

// 1-withoutError nil
func TestJobBatch1WithoutErrorNoLabor(t *testing.T) {
	jt := newJobTester(&batch1WithoutError{}, nil)
	with := []interface{}{1, 2, 3}
	assert.Equal(t, 1, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, len(with), len(allDone))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, true, common.IsIn(done.R, with))
		assert.Equal(t, nil, done.E)
	}
}

// 1-withoutError withError
func TestJobBatch1WithoutErrorLaborWithError(t *testing.T) {
	jt := newJobTester(&batch1WithoutError{}, &laborWithError{})
	with := []interface{}{1, 2, 3}
	assert.Equal(t, 1, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, len(with), len(allDone))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, nil, done.R)
		assert.Equal(t,
			fmt.Sprintf("× labor failed ( %+v, test laborWithError )", done.P),
			done.E.Error())
	}
}

// 1-withoutError withoutError
func TestJobBatch1WithoutErrorLaborWithoutError(t *testing.T) {
	jt := newJobTester(&batch1WithoutError{}, &laborWithoutError{})
	with := []interface{}{1, 2, 3}
	assert.Equal(t, 1, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, len(with), len(allDone))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, true, common.IsIn(done.R, with))
		assert.Equal(t, nil, done.E)
	}
}

// 3-withError nil
func TestJobBatchNWithErrorNoLabor(t *testing.T) {
	jt := newJobTester(&batchNWithError{3}, nil)
	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	assert.Equal(t, 3, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, len(with), len(allDone))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, nil, done.R)
		assert.Equal(t, true, strings.Contains(done.E.Error(), "× reduce failed ( ["))
		assert.Equal(t, true, strings.Contains(done.E.Error(), fmt.Sprintf("%+v", done.P)))
		assert.Equal(t, true, strings.Contains(done.E.Error(), "], test batch3WithError )"))
	}
}

// 3-withError withError
func TestJobBatchNWithErrorLaborWithError(t *testing.T) {
	jt := newJobTester(&batchNWithError{3}, &laborWithError{})
	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	assert.Equal(t, 3, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, len(with), len(allDone))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, nil, done.R)
		assert.Equal(t, true, strings.Contains(done.E.Error(), "× reduce failed ( ["))
		assert.Equal(t, true, strings.Contains(done.E.Error(), fmt.Sprintf("%+v", done.P)))
		assert.Equal(t, true, strings.Contains(done.E.Error(), "], test batch3WithError )"))
	}
}

// 3-withError withoutError
func TestJobBatchNWithErrorLaborWithoutError(t *testing.T) {
	jt := newJobTester(&batchNWithError{3}, &laborWithoutError{})
	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	assert.Equal(t, 3, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, len(with), len(allDone))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, nil, done.R)
		assert.Equal(t, true, strings.Contains(done.E.Error(), "× reduce failed ( ["))
		assert.Equal(t, true, strings.Contains(done.E.Error(), fmt.Sprintf("%+v", done.P)))
		assert.Equal(t, true, strings.Contains(done.E.Error(), "], test batch3WithError )"))
	}
}

// 3-withoutError nil
func TestJobBatchNWithoutErrorNoLabor(t *testing.T) {
	jt := newJobTester(&batchNWithoutError{3}, nil)
	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	assert.Equal(t, 3, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, true, common.IsIn(done.R, with))
		assert.Equal(t, nil, done.E)
	}
}

// 3-withoutError withError
func TestJobBatchNWithoutErrorLaborWithError(t *testing.T) {
	jt := newJobTester(&batchNWithoutError{3}, &laborWithError{})
	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	assert.Equal(t, 3, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, nil, done.R)
		assert.Equal(t, fmt.Sprintf("× labor failed ( %+v, test laborWithError )", done.P), done.E.Error())
	}
}

// 3-withoutError withoutError
func TestJobBatchNWithoutErrorLaborWithoutError(t *testing.T) {
	jt := newJobTester(&batchNWithoutError{3}, &laborWithoutError{})
	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	assert.Equal(t, 3, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, true, common.IsIn(done.R, with))
		assert.Equal(t, nil, done.E)
	}
}

// BatchHandler LaborHandler RetryHandler ErrorHandler are all job itself
func (jt *JobTester) Size() int { return jt.maxRetry }
func (jt *JobTester) Reduce(ctx context.Context, groupedMash []interface{}) (interface{}, error) {
	return groupedMash[len(groupedMash)-1], nil
}
func (jt *JobTester) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	v, _ := p.(int)
	if v%2 == 0 {
		return p, nil
	} else {
		return nil, fmt.Errorf("itself error")
	}
}

func TestJobItself(t *testing.T) {
	jt := &JobTester{NewJob(logging.GetLogger(WHAT), runtime.NumCPU()), 3, 0}
	jt.BatchStrategy(jt).LaborStrategy(jt).RetryStrategy(jt).ErrorStrategy(jt)
	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	assert.Equal(t, jt.maxRetry, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, jt.maxRetry, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		v, _ := done.P.(int)
		if v%2 == 1 {
			assert.Equal(t, nil, done.R)
			assert.Equal(t, fmt.Sprintf("× labor failed ( %v, itself error )", done.P), done.E.Error())
		} else {
			assert.Equal(t, true, common.IsIn(done.R, with))
			assert.Equal(t, nil, done.E)
		}
	}
}

type retryHook struct{}

func (rh *retryHook) Worth(Done) bool {
	return false
}

func (rh *retryHook) Forgo() bool {
	return true
}

func TestJobItselfWithoutRetry(t *testing.T) {
	jt := &JobTester{NewJob(logging.GetLogger(WHAT), runtime.NumCPU()), 3, 0}
	jt.BatchStrategy(jt).LaborStrategy(jt).RetryStrategy(&retryHook{}).ErrorStrategy(jt)
	with := []interface{}{1, 2, 3, 4, 5, 6, 7, 8}
	assert.Equal(t, jt.maxRetry, jt.batchSize())
	allDone := jt.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, 0, jt.curRetry)
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		v, _ := done.P.(int)
		if v%2 == 1 {
			assert.Equal(t, nil, done.R)
			assert.Equal(t, fmt.Sprintf("× labor failed ( %v, itself error )", done.P), done.E.Error())
		} else {
			assert.Equal(t, true, common.IsIn(done.R, with))
			assert.Equal(t, nil, done.E)
		}
	}
}

type blindlyRetryJob struct {
	*Job
	maxRetry, curRetry int
}

func (bj *blindlyRetryJob) Worth(Done) bool {
	return true // blindly return true
}

func (bj *blindlyRetryJob) Forgo() bool {
	bj.curRetry++
	return bj.curRetry >= bj.maxRetry
}

func (bj *blindlyRetryJob) Work(ctx context.Context, p interface{}) (r interface{}, e error) {
	return nil, nil
}

func TestBlindlyRetryJob(t *testing.T) {
	brj := &blindlyRetryJob{NewJob(logging.GetLogger(WHAT), runtime.NumCPU()), 3, 0}
	brj.LaborStrategy(brj).RetryStrategy(brj)
	assert.Equal(t, 1, brj.batchSize())
	with := []interface{}{"blindly retry job"}
	allDone := brj.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, brj.maxRetry, brj.curRetry)
	assert.Equal(t, 1, len(allDone))
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, nil, done.R)
		assert.Equal(t, nil, done.E)
	}
}

func TestBlindlyRetryJobWithNil(t *testing.T) {
	brj := &blindlyRetryJob{NewJob(logging.GetLogger(WHAT), runtime.NumCPU()), 5, 0}
	brj.LaborStrategy(brj).RetryStrategy(brj)
	assert.Equal(t, 1, brj.batchSize())
	with := []interface{}{nil}
	allDone := brj.Run(context.Background(), NewDataSupplier(with))
	assert.Equal(t, brj.maxRetry, brj.curRetry)
	assert.Equal(t, 1, len(allDone))
	for _, done := range allDone {
		assert.Equal(t, true, common.IsIn(done.P, with))
		assert.Equal(t, nil, done.R)
		assert.Equal(t, nil, done.E)
	}
}
