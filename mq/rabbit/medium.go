package rabbit

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
)

type medium struct {
	ctx         context.Context
	cancel      context.CancelFunc
	established sync.Map
	terminated  sync.Map
	teared      sync.Map
	logger      logrus.FieldLogger
}

func (c *medium) Context() context.Context        { return c.ctx }
func (c *medium) tearDown(teared chan<- struct{}) { teared <- struct{}{} }
func (c *medium) Notify(events *sync.Map) {
	events.Range(func(key, value interface{}) bool {
		if ch, ok := value.(chan struct{}); !ok {
			panic("Notify")
		} else {
			ch <- struct{}{}
		}
		return true
	})
}
func (c *medium) Accept(key string) (established, terminated, teared chan struct{}) {
	elem, ok := c.established.LoadOrStore(key, make(chan struct{}, 1))
	if established, ok = elem.(chan struct{}); !ok {
		panic("Accept established")
	}
	elem, ok = c.terminated.LoadOrStore(key, make(chan struct{}, 1))
	if terminated, ok = elem.(chan struct{}); !ok {
		panic("Accept terminated")
	}
	elem, ok = c.teared.LoadOrStore(key, make(chan struct{}, 1))
	if teared, ok = elem.(chan struct{}); !ok {
		panic("Accept teared")
	}
	return
}
func (c *medium) Remove(key string) {
	c.established.Delete(key)
	c.terminated.Delete(key)
	c.teared.Delete(key)
}
func (c *medium) Await() {
	c.teared.Range(func(key, value interface{}) bool {
		if teared, ok := value.(chan struct{}); !ok {
			panic("Await teared")
		} else {
			<-teared
		}
		return true
	})
}
func (c *medium) Clean() {
	clear := func(events *sync.Map) {
		events.Range(func(key, value interface{}) bool {
			if ch, ok := value.(chan struct{}); !ok {
				panic("Clean")
			} else {
				close(ch)
				events.Delete(key)
			}
			return true
		})
	}
	clear(&c.established)
	clear(&c.terminated)
	clear(&c.teared)
}

func NewMedium(ctx context.Context, logger logrus.FieldLogger) *medium {
	childCtx, cancelFn := context.WithCancel(ctx)
	return &medium{
		ctx:    childCtx,
		cancel: cancelFn,
		logger: logger,
	}
}
