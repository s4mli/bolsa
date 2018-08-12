package rebbit

import (
	"context"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

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

func (c *Consumer) connect(user, password, uri string) {
	c.ready.Store(false)
	c.qConn, c.qChan = connect(c.logger, user, password, uri, c.connect)
	c.ready.Store(true)
}

func (c *Consumer) Close() {
	c.qConn.Close()
	c.ready.Store(false)
}

func (c *Consumer) Run() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGILL, syscall.SIGSYS,
		syscall.SIGTERM, syscall.SIGTRAP, syscall.SIGQUIT, syscall.SIGABRT)
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				c.logger.Errorf("⏳ cancellation, %s consumer quiting...", c.qName)
				c.Close()
				return
			case s := <-sig:
				c.logger.Errorf("⏳ signal ( %+v ) %s consumer quiting...", s, c.qName)
				c.Close()
				return
			default:
				time.Sleep(time.Millisecond * 10)
			}
		}
	}()

	for i := 0; i < c.qWorkers; i++ {
		go func() {
			if ready, ok := c.ready.Load().(bool); ok && ready {
				consume(c.qChan, c.qName, c.handler)
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
