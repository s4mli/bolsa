package job

import (
	"context"
	"fmt"
)

///////////
// Job //
type job interface {
	Run(ctx context.Context, with []interface{}) []Done
}

// batch for feeding
type batchHandler interface {
	Size() int
	Batch(context.Context, []interface{}) (interface{}, error)
}

// action for chewing
type actionHandler interface {
	Act(ctx context.Context, p interface{}) (r interface{}, e error)
}

// retry for Job
type retryHandler interface {
	Worth(Done) bool
	Forgo() bool
}

type errorHandler interface {
	OnError(error)
}

// result for a job which contains Parameter Result and Error
type Done struct {
	P, R interface{}
	E    error
}

func (d *Done) String() string {
	return fmt.Sprintf(" * Params: %+v\n * Result: ( %+v , %+v )", d.P, d.R, d.E)
}
