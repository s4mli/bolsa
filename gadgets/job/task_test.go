package job

import (
	"fmt"
	"testing"

	"github.com/samwooo/bolsa/gadgets/logging"
	"github.com/stretchr/testify/assert"
)

func testWithNullPayload(t *testing.T) {
	logging.SetupLogger("", &fakeLogHandler{}, &fakeLogFilter{})
	cs := fakeContextStreamer{}
	task := NewTask(&fakeActionWithError{}, &cs,
		logging.GetLogger("shutUp"))

	var c []Context
	payload, err := cs.ContextsToBytes(c)
	assert.Equal(t, err, nil)
	r, err := task.Run(&fakeMessage{payload: payload})
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(r))
}

func testWithOnePayload(t *testing.T) {
	logging.SetupLogger("", &fakeLogHandler{}, &fakeLogFilter{})
	cs := fakeContextStreamer{}
	task := NewTask(&fakeAction{}, &cs,
		logging.GetLogger("shutUp"))

	var c []Context
	c = append(c, fakeContext{111})
	payload, err := cs.ContextsToBytes(c)
	assert.Equal(t, err, nil)
	r, err := task.Run(&fakeMessage{payload: payload})
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(r))
	cc, ok := r[0].C.(fakeContext)
	assert.Equal(t, true, ok)
	assert.Equal(t, 111, cc.Id)
}

func testWithMultiplePayloads(t *testing.T) {
	logging.SetupLogger("", &fakeLogHandler{}, &fakeLogFilter{})
	cs := fakeContextStreamer{}
	task := NewTask(&fakeAction{}, &cs,
		logging.GetLogger("shutUp"))

	var c []Context
	ids := []interface{}{111, 222, 333, 444, 555}
	for _, v := range ids {
		c = append(c, fakeContext{v.(int)})
	}
	payload, err := cs.ContextsToBytes(c)
	assert.Equal(t, err, nil)
	r, err := task.Run(&fakeMessage{payload: payload})
	assert.Equal(t, nil, err)
	assert.Equal(t, len(ids), len(r))
	for index := 0; index < len(ids); index++ {
		cc, ok := r[index].C.(fakeContext)
		assert.Equal(t, true, ok)
		assert.Equal(t, true, isIn(cc.Id, ids))
	}
}

func testWithOneError(t *testing.T) {
	logging.SetupLogger("", &fakeLogHandler{}, &fakeLogFilter{})
	cs := fakeContextStreamer{}
	task := NewTask(&fakeActionWithError{}, &cs,
		logging.GetLogger("shutUp"))

	var c []Context
	c = append(c, fakeContext{111})
	payload, err := cs.ContextsToBytes(c)
	assert.Equal(t, err, nil)
	r, err := task.Run(&fakeMessage{payload: payload})
	assert.Equal(t, 1, len(r))
	assert.Equal(t, nil, err)
	assert.Equal(t, "nice error: {\"Id\":111}", r[0].E.Error())
}

func testWithMultipleErrors(t *testing.T) {
	logging.SetupLogger("", &fakeLogHandler{}, &fakeLogFilter{})
	cs := fakeContextStreamer{}
	task := NewTask(&fakeActionWithError{}, &cs,
		logging.GetLogger("shutUp"))

	var c []Context
	var errors []interface{}
	ids := []interface{}{111, 222, 333, 444, 555, 666, 777, 888, 999}
	for _, v := range ids {
		c = append(c, fakeContext{v.(int)})
		errors = append(errors, fmt.Sprintf("nice error: {\"Id\":%d}", v))
	}
	payload, err := cs.ContextsToBytes(c)
	assert.Equal(t, err, nil)
	r, err := task.Run(&fakeMessage{payload: payload})
	assert.Equal(t, len(ids), len(r))
	assert.Equal(t, nil, err)
	for index := 0; index < len(ids); index++ {
		assert.Equal(t, true, isIn(r[index].E.Error(), errors))
	}
}

func TestTask(t *testing.T) {
	testWithOneError(t)
	testWithMultipleErrors(t)
	testWithNullPayload(t)
	testWithOnePayload(t)
	testWithMultiplePayloads(t)
}
