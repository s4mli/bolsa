package sqs

import (
	"context"
	"math"
	"os"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/logging"
)

type Consumer struct {
	ctx      context.Context
	logger   logging.Logger
	qService *sqs.SQS
	qUrl     string
	qWait    int64
	qWorkers int
	handler  MessageHandler
	ready    atomic.Value
}

func (c *Consumer) connect(qRegion string) {
	c.ready.Store(false)
	c.qService = connect(c.logger, qRegion)
	c.ready.Store(true)
}

func (c *Consumer) Close() { c.ready.Store(false) }

func (c *Consumer) Run() {
	common.TerminateIf(c.ctx,
		func() {
			c.logger.Infof("cancellation, ( ...%s ) terminated", c.qUrl[int(math.Max(
				float64(len(c.qUrl)-9), float64(0))):])
			c.Close()
		},
		func(s os.Signal) {
			c.logger.Infof("signal ( %+v ), ( ...%s ) terminated", s, c.qUrl[int(math.Max(
				float64(len(c.qUrl)-9), float64(0))):])
			c.Close()
		})

	for i := 0; i < c.qWorkers; i++ {
		go func() {
			if ready, ok := c.ready.Load().(bool); ok && ready {
				consume(c.ctx, c.logger, c.qService, c.qUrl, c.qWait, c.handler)
			}
		}()
	}
}

func NewConsumer(ctx context.Context, qRegion, qUrl string, qWait int64, qWorkers int,
	handler MessageHandler) *Consumer {
	c := &Consumer{ctx: ctx, logger: logging.GetLogger(" ⓠ "), qUrl: qUrl, qWait: qWait, qWorkers: qWorkers,
		handler: handler, ready: atomic.Value{}}
	c.connect(qRegion)
	return c
}
