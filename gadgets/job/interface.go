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

// group for feeding
type batchNeeded interface {
	batchSize() int
	doBatch(context.Context, []interface{}) (interface{}, error)
}

// action for chewing
type actionNeeded interface {
	doAction(ctx context.Context, p interface{}) (r interface{}, e error)
}

// retry for Job
type retryNeeded interface {
	worthRetry(Done) bool
	forgoRetry() bool
}

// result for a job which contains Parameter Result and Error
type Done struct {
	P, R interface{}
	E    error
}

func (d *Done) String() string {
	return fmt.Sprintf(" * Params: %+v\n * Result: ( %+v , %+v )", d.P, d.R, d.E)
}
