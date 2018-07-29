package share

import "context"

/////////////////////
// Error Strategy //
type ErrorStrategy interface {
	OnError(Done)
}

/////////////////////
// Batch Strategy //
type BatchStrategy interface {
	Batch() int
}

/////////////////////
// Labor Strategy //
type LaborStrategy interface {
	Work(ctx context.Context, p interface{}) (r interface{}, e error)
}

/////////////////////
// Retry Strategy //
type RetryStrategy interface {
	Worth(Done) bool
	Limit() int
}
