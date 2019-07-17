package rabbit

import (
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

type MessageHandler func(amqp.Table, []byte) error

func (mh MessageHandler) handle(headers amqp.Table, body []byte) error { return mh(headers, body) }

type consumer struct {
	connection *Connection
	workers    map[int]*consumerWorker
}

func newConsumer(
	ctx context.Context,
	uri, user, password, name string,
	prefetchCount, prefetchSize, count int,
	handler MessageHandler,
	logger logrus.FieldLogger,
) *consumer {
	connection := NewConnection(ctx, uri, user, password, logger)
	c := &consumer{
		connection: connection,
		workers: func() map[int]*consumerWorker {
			workers := make(map[int]*consumerWorker, count)
			for id := 0; id < count; id++ {
				workers[id] = newConsumerWorker(
					NewChannel(id+1, connection, prefetchCount, prefetchSize, logger),
					id+1, name, handler, logger,
				).run()
			}
			return workers
		}(),
	}
	connection.Start()
	return c
}

type RetryLimitReached func(exchange, queue string, routeKeys []interface{}, body []byte)

func (rlr RetryLimitReached) handle(exchange, queue string, routeKeys []interface{}, body []byte) {
	rlr(exchange, queue, routeKeys, body)
}

type retryableConsumer struct {
	*consumer
	limit int
}

func RunConsumer(
	ctx context.Context,
	uri, user, password, name string,
	prefetchCount, prefetchSize, count, limit int,
	handler MessageHandler,
	rlr RetryLimitReached,
	logger logrus.FieldLogger,
) *retryableConsumer {
	retryableHandler := func() MessageHandler {
		return func(headers amqp.Table, body []byte) error {
			if xDeaths, ok := headers["x-death"]; ok {
				xDeaths, _ := xDeaths.([]interface{})
				xDeath, _ := xDeaths[1].(amqp.Table)
				exchange, _ := xDeath["exchange"].(string)
				queue, _ := xDeath["queue"].(string)
				routeKeys, _ := xDeath["routing-keys"].([]interface{})
				retried, _ := xDeath["count"].(int64)
				if limit > 0 && int(retried) >= limit {
					if rlr != nil {
						rlr.handle(exchange, queue, routeKeys, body)
					}
					return nil
				} else {
					return handler.handle(headers, body)
				}
			} else {
				return handler.handle(headers, body)
			}
		}
	}

	rc := &retryableConsumer{
		consumer: newConsumer(ctx, uri, user, password, name, prefetchCount, prefetchSize,
			count, retryableHandler(), logger.WithField("Retry", limit)),
		limit: limit,
	}
	return rc
}
