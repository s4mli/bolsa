package job

type ActionStatus int

const (
	FAILED ActionStatus = iota
)

type Context interface{}
type ContextStreamer interface {
	ContextsToBytes([]Context) ([]byte, error)
	BytesToContexts([]byte) ([]Context, error)
}

type ActionResult struct {
	C Context
	S ActionStatus
	E error
}
type Action interface {
	Act(Context) ActionResult
}

type TaskResult []ActionResult
type TaskRescue interface {
	Rescue(TaskResult)
}
