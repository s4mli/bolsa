package rabbit

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/logging"
	"github.com/streadway/amqp"
)

type Consumer struct {
	ctx      context.Context
	logger   logging.Logger
	qConn    atomic.Value
	qChan    atomic.Value
	qName    string
	qWorkers int
	handler  MessageHandler
}

func (c *Consumer) connect(qUser, qPassword, qUri string) {
	qConn, qChan := connect(c.logger, qUser, qPassword, qUri, c.connect)
	c.qConn.Store(qConn)
	c.qChan.Store(qChan)
}

func (c *Consumer) Close() {
	for {
		if conn, ok := c.qConn.Load().(*amqp.Connection); ok && conn != nil {
			conn.Close()
			c.logger.Infof("consumer %s closed", c.qName)
			break
		} else {
			c.logger.Warn("close cast failed")
			time.Sleep(time.Millisecond * 50)
		}
	}
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
			if qChan, ok := c.qChan.Load().(*amqp.Channel); ok && qChan != nil {
				consume(c.logger, qChan, c.qName, c.handler)
			} else {
				c.logger.Error("consume cast failed")
				time.Sleep(time.Millisecond * 10)
			}
		}()
	}
}

func (c *Consumer) Publish(exchange, topic string, body []byte) error {
	if qChan, ok := c.qChan.Load().(*amqp.Channel); ok && qChan != nil {
		return publish(c.logger, qChan, exchange, topic, body)
	} else {
		msg := "publish cast failed"
		c.logger.Error(msg)
		return fmt.Errorf(msg)
	}
}

func NewConsumer(ctx context.Context, logger logging.Logger, qName, qUser, qPassword, qUri string, qWorkers int,
	handler MessageHandler) *Consumer {
	c := &Consumer{ctx: ctx, logger: logger, qName: qName, qWorkers: qWorkers, handler: handler}
	c.connect(qUser, qPassword, qUri)
	return c
}
