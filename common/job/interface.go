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
		return "reduce"
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

////////////////////////
// Task & Job Result //
type Done struct {
	P interface{} // parameter
	R interface{} // result
	E error       // error
	X interface{} // anything as u wish, normally is nil
}

func (d *Done) String() string {
	return fmt.Sprintf("\n * P: %+v\n * X: %+v\n * R , E: ( %+v , %+v )\n", d.P, d.X, d.R, d.E)
}

func newDone(para interface{}, result interface{}, err error, extra interface{}) Done {
	return Done{para, result, err, extra}
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
	OnError(Done)
}

////////////////////////
// Job data supplier //
type supplier interface {
	Adapt() <-chan Done
}

//////////
// Job //
type Job struct {
	Logger  logging.Logger
	workers int
	batchStrategy
	laborStrategy
	retryStrategy
	errorStrategy
}
