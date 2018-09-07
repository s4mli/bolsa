package rabbit

import (
	"context"

	"github.com/samwooo/bolsa/logging"
	"github.com/streadway/amqp"
)

//////////////////////////////////
// Retryable RabbitMQ Consumer //
// Requires a bit of Ops works:
// 1: Topic Exchange=E with Qs
// 2: Each Q has DLX=DLE and DLK but NOT TTL which by calling Nack(requeue:false) msg will be move into DLE immediately
// 3: DL Topic Exchange=DLE with DLQs
// 4: Each DLQ has a TTL and DLX=E with DLK which will redirect msg back to Qs when TTL expired, see x-death in headers
type RetryableConsumer struct {
	*Consumer
	limit int
}

func NewRetryableConsumer(ctx context.Context, logger logging.Logger, qName, qUser, qPassword, qUri string,
	qWorkers int, handler MessageHandler, limit int) *RetryableConsumer {
	retryableHandler := func() MessageHandler {
		return func(headers amqp.Table, body []byte) error {
			if xDeaths, ok := headers["x-death"]; ok {
				xDeaths, _ := xDeaths.([]interface{})
				xDeath, _ := xDeaths[0].(amqp.Table)
				retried, _ := xDeath["count"].(int64)
				if int(retried) >= limit {
					logger.Warnf("reached retry limit ( %d ) drop message", limit)
					return nil
				} else {
					return handler.handle(headers, body)
				}
			} else {
				return handler.handle(headers, body)
			}
		}
	}
	c := &RetryableConsumer{
		NewConsumer(ctx, logger, qName, qUser, qPassword, qUri, qWorkers, retryableHandler()),
		limit,
	}
	c.connect(qUser, qPassword, qUri)
	return c
}
