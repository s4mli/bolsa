package share

import "context"

/////////////////////
// Labor Strategy //
type LaborStrategy interface {
	Work(ctx context.Context, p interface{}) (r interface{}, e error)
}
type Labor func(ctx context.Context, p interface{}) (r interface{}, e error)

func (fn Labor) Work(ctx context.Context, p interface{}) (r interface{}, e error) { return fn(ctx, p) }

/////////////////////
// Retry Strategy //
type RetryStrategy interface {
	Worth(Done) bool
	Limit() int
}
