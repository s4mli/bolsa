package rabbit

import (
	"context"
	"os"
	"sync/atomic"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/logging"
	"github.com/streadway/amqp"
)

type Consumer struct {
	ctx      context.Context
	logger   logging.Logger
	qConn    *amqp.Connection
	qChan    *amqp.Channel
	qName    string
	qWorkers int
	handler  MessageHandler
	ready    atomic.Value
}

func (c *Consumer) connect(qUser, qPassword, qUri string) {
	c.ready.Store(false)
	c.qConn, c.qChan = connect(c.logger, qUser, qPassword, qUri, c.connect)
	c.ready.Store(true)
}

func (c *Consumer) Close() {
	c.qConn.Close()
	c.ready.Store(false)
}

func (c *Consumer) Run() {
	common.TerminateIf(c.ctx,
		func() {
			c.logger.Infof("⏳ cancellation, consumer ( %s ) quiting...", c.qName)
			c.Close()
		},
		func(s os.Signal) {
			c.logger.Infof("⏳ signal ( %+v ) consumer ( %s ) quiting...", s, c.qName)
			c.Close()
		})

	for i := 0; i < c.qWorkers; i++ {
		go func() {
			if ready, ok := c.ready.Load().(bool); ok && ready {
				consume(c.logger, c.qChan, c.qName, c.handler)
			}
		}()
	}
}

func NewConsumer(ctx context.Context, logger logging.Logger, qName, qUser, qPassword, qUri string, qWorkers int,
	handler MessageHandler) *Consumer {
	c := &Consumer{ctx: ctx, logger: logger, qName: qName, qWorkers: qWorkers, handler: handler, ready: atomic.Value{}}
	c.connect(qUser, qPassword, qUri)
	return c
}
