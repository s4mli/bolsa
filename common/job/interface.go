package job

import (
	"context"
	"fmt"

	"github.com/samwooo/bolsa/common/logging"
)

type strategyType int

const (
	typeBatch strategyType = iota
	typeLabor
	typeRetry
)

func (ht *strategyType) String() string {
	switch *ht {
	case typeBatch:
		return "batch"
	case typeLabor:
		return "labor"
	case typeRetry:
		return "retry"
	}
	return "?"
}

////////////////
// Job Error //
type Error struct {
	st strategyType
	error
}

func (je Error) Error() string {
	return fmt.Sprintf("× %s failed %s", je.st.String(), je.error.Error())
}

func newError(st strategyType, err error) *Error {
	return &Error{st, err}
}

/////////////////
// Job Result //
type Done struct {
	P, R interface{}
	E    error
}

func (d *Done) String() string {
	return fmt.Sprintf(" * Params: %+v\n * Result: ( %+v , %+v )", d.P, d.R, d.E)
}

/////////////////////
// batch strategy //
type batchStrategy interface {
	Size() int
	Reduce(context.Context, []interface{}) (interface{}, error)
}

/////////////////////
// labor strategy //
type laborStrategy interface {
	Work(ctx context.Context, p interface{}) (r interface{}, e error)
}

/////////////////////
// retry strategy //
type retryStrategy interface {
	Worth(Done) bool
	Forgo() bool
}

/////////////////////
// error strategy //
type errorStrategy interface {
	OnError(error)
}

////////////////////////
// Job data supplier //
type Supplier interface {
	// Job will drain data from a Supplier running in a single goroutine
	Drain(context.Context) (interface{}, error)
	Empty() bool
}

//////////
// Job //
/* 1. Drain data from Supplier 	( supplier.Drain() )		( 1 worker )	*/
/* 2. Group data into batch 	( batchStrategy.Size() )	( 1 worker )	*/
/* 3. Reduce batched data 		( batchStrategy.Reduce() )	( N workers )	*/
/* 4. Work on reduced data 		( laborStrategy.Work() )	( N workers )	*/
/* 5. Retry batch & labor failures on 2 new Jobs 			( N workers )	*/
type Job struct {
	Logger  logging.Logger
	workers int
	batchStrategy
	laborStrategy
	retryStrategy
	errorStrategy
}
