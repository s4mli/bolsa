package model

/////////////////////
// Labor Strategy //
type LaborStrategy interface {
	Work(p interface{}) (r interface{}, e error)
}
type Labor func(p interface{}) (r interface{}, e error)

func (fn Labor) Work(p interface{}) (r interface{}, e error) { return fn(p) }

/////////////////////
// Retry Strategy //
type RetryStrategy interface {
	Worth(Done) bool
	Limit() int
}
