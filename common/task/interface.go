package task

import "context"

type task interface {
	Do(context.Context, interface{}) error
	Scale(workers int)
}
