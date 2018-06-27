package job

import (
	"context"
	"fmt"

	"github.com/samwooo/bolsa/common/logging"
)

type hookType int

const (
	Batch hookType = iota
	Action
	Retry
)

func (ht *hookType) String() string {
	switch *ht {
	case Batch:
		return "batch"
	case Action:
		return "action"
	case Retry:
		return "retry"
	}
	return "?"
}

//////////
// Job //
type Job struct {
	Logger  logging.Logger
	workers int
	batchHandler
	actionHandler
	retryHandler
	errorHandler
}

/////////////////
// batch hook //
type batchHandler interface {
	Size() int
	Batch(context.Context, []interface{}) (interface{}, error)
}

//////////////////
// action hook //
type actionHandler interface {
	Act(ctx context.Context, p interface{}) (r interface{}, e error)
}

/////////////////
// retry hook //
type retryHandler interface {
	Worth(Done) bool
	Forgo() bool
}

/////////////////
// error hook //
type errorHandler interface {
	OnError(error)
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

////////////////
// Job Error //
type Error struct {
	hook hookType
	error
}

func (je Error) Error() string {
	return fmt.Sprintf("× %s failed %s", je.hook.String(), je.error.Error())
}

func newError(ht hookType, err error) *Error {
	return &Error{ht, err}
}
