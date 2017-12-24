package job

import (
	"encoding/json"
	"fmt"
	"time"

	"sync"

	"github.com/samwooo/bolsa/gadgets/logging"
	"github.com/samwooo/bolsa/gadgets/queue"
)

func isIn(id interface{}, ids []interface{}) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

////////////////////////////////////////////////////////////////////////////////////////////////////
type fakeLogFilter struct{}

func (h *fakeLogFilter) WorthEmit(lvl logging.LogLevel, msg interface{}) (bool, interface{}) {
	return false, msg
}

type fakeLogHandler struct{}

func (h *fakeLogHandler) Emit(lvl logging.LogLevel, prefix string, msg interface{}) {}

////////////////////////////////////////////////////////////////////////////////////////////////////
type fakeAction struct{}

func (a *fakeAction) Act(c Context) ActionResult {
	return ActionResult{C: c, S: FAILED, E: nil}
}

type fakeActionWithError struct{}

func (a *fakeActionWithError) Act(c Context) ActionResult {
	if jc, err := json.Marshal(c); err != nil {
		return ActionResult{C: nil, S: FAILED,
			E: fmt.Errorf("marshal error")}
	} else {
		return ActionResult{C: nil, S: FAILED,
			E: fmt.Errorf("nice error: %s", string(jc))}
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
type fakeContext struct {
	Id int
}

type fakeContextStreamer struct{}

func (n *fakeContextStreamer) ContextsToBytes(cs []Context) ([]byte, error) {
	return json.Marshal(cs)
}

func (n *fakeContextStreamer) BytesToContexts(bs []byte) ([]Context, error) {
	var contexts []fakeContext
	if err := json.Unmarshal(bs, &contexts); err != nil {
		return nil, err
	} else {
		var cs []Context
		for _, t := range contexts {
			cs = append(cs, t)
		}
		return cs, nil
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
type fakeMessage struct {
	payload []byte
}

func (m *fakeMessage) Payload() []byte {
	return m.payload
}

func (m *fakeMessage) ChangeVisibility(remainingInvisibility time.Duration) error {
	return nil
}

func (m *fakeMessage) Delete() error {
	return nil
}

type fakeQueue struct {
	messageCount int32
	mutex        sync.Mutex
}

func (q *fakeQueue) SendMessage(delay int, payload []byte) error {
	return nil
}

func (q *fakeQueue) ReceiveMessage() (queue.Message, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	cs := []Context{fakeContext{int(q.messageCount)}}
	s := fakeContextStreamer{}
	bs, _ := s.ContextsToBytes(cs)
	return &fakeMessage{bs}, nil

}

func (q *fakeQueue) setMessageCount(c int32) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.messageCount = c
}

func (q *fakeQueue) getMessageCount() int32 {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.messageCount
}

////////////////////////////////////////////////////////////////////////////////////////////////////
type fakeTaskRescue struct {
	*Job
}

func (r *fakeTaskRescue) Rescue(tr TaskResult) {
	fq, _ := r.q.(*fakeQueue)
	fq.setMessageCount(666)
}
