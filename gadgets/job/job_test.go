package job

import (
	"testing"
	"time"

	"sync"

	"github.com/samwooo/bolsa/gadgets/logging"
	"github.com/stretchr/testify/assert"
)

func testFeed(t *testing.T) {
	logging.SetupLogger("", &fakeLogHandler{}, &fakeLogFilter{})
	cs := fakeContextStreamer{}
	jt := NewJob(&fakeAction{}, &cs, &fakeTaskRescue{}, &fakeQueue{1, sync.Mutex{}},
		nil, 1, 1, logging.GetLogger("shutUp"))

	bs, err := cs.BytesToContexts((<-jt.feed()).Payload())
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(bs))
	assert.Equal(t, fakeContext{1}, bs[0])
}

func testChew(t *testing.T) {
	logging.SetupLogger("", &fakeLogHandler{}, &fakeLogFilter{})
	cs := fakeContextStreamer{}
	jt := NewJob(&fakeAction{}, &cs, &fakeTaskRescue{}, &fakeQueue{1, sync.Mutex{}},
		nil, 1, 1, logging.GetLogger("shutUp"))

	rs := <-jt.chew(jt.feed())
	assert.Equal(t, 1, len(rs))
	assert.Equal(t, nil, rs[0].E)
	assert.Equal(t, FAILED, rs[0].S)
	c, ok := rs[0].C.(fakeContext)
	assert.Equal(t, true, ok)
	assert.Equal(t, 1, c.Id)
}

func testDigest(t *testing.T) {
	logging.SetupLogger("", &fakeLogHandler{}, &fakeLogFilter{})
	cs := fakeContextStreamer{}
	jt := NewJob(&fakeAction{}, &cs, nil, &fakeQueue{1, sync.Mutex{}},
		nil, 1, 1, logging.GetLogger("shutUp"))
	jt.TaskRescue = &fakeTaskRescue{jt}
	jt.digest(jt.chew(jt.feed()))
	time.Sleep(time.Second)
	fq, ok := jt.q.(*fakeQueue)
	assert.Equal(t, true, ok)
	assert.Equal(t, int32(666), fq.getMessageCount())
}

func TestJob(t *testing.T) {
	testFeed(t)
	testChew(t)
	testDigest(t)
}
