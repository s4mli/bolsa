package rabbit

import (
	"time"

	"github.com/samwooo/bolsa/logging"
	"github.com/streadway/amqp"
)

type MessageHandler func(amqp.Table, []byte) error

func (mh MessageHandler) handle(headers amqp.Table, body []byte) error { return mh(headers, body) }

type consumer struct {
	Id            int
	logger        logging.Logger
	qChan         *amqp.Channel
	qName         string
	prefetchCount int
	prefetchSize  int
	handler       MessageHandler
}

func (c *consumer) consume(qChan *amqp.Channel) {
	if c.qChan != nil {
		c.qChan.Close()
	}
	c.qChan = qChan
	if msgCh, err := c.qChan.Consume(c.qName,
		"",
		false,
		false,
		false,
		false, nil); err != nil {
		c.logger.Errorf("( %d ) consume failed ( %s )", c.Id, err.Error())
	} else {
		for m := range msgCh {
			if err := c.handler.handle(m.Headers, m.Body); err != nil {
				c.logger.Errorf("( %d ) handle message ( %s ) failed ( %s )", c.Id, string(m.Body), err.Error())
				if e := m.Nack(false, false); e != nil {
					c.logger.Errorf("( %d ) nack ( %s ) failed ( %s )", c.Id, string(m.Body), e.Error())
				}
			} else {
				c.logger.Debugf("( %d ) handle message ( %s ) succeed", c.Id, string(m.Body))
				if e := m.Ack(false); e != nil {
					c.logger.Errorf("( %d ) ack ( %s ) failed ( %s )", c.Id, string(m.Body), e.Error())
				}
			}
		}
	}
}

func (c *consumer) run(conn *Connection) {
	connected, reconnecting, closed := conn.register(c)
	go func(connected, reconnecting, closed chan struct{}) {
		for {
			select {
			case <-closed:
				c.logger.Infof("( %d ) exiting", c.Id)
				return
			case <-reconnecting:
				c.logger.Infof("( %d ) wait for reconnecting", c.Id)
				time.Sleep(50 * time.Millisecond)
			case <-connected:
				if qChan, err := conn.channel(c.prefetchCount, c.prefetchSize); err != nil {
					c.logger.Errorf("( %d ) %s", c.Id, err.Error())
				} else {
					c.logger.Infof("( %d ) channel refreshed", c.Id)
					c.consume(qChan)
				}
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}
	}(connected, reconnecting, closed)
}

func newConsumer(id int, logger logging.Logger, qName string, prefetchCount, prefetchSize int,
	handler MessageHandler) *consumer {
	return &consumer{id, logger, nil, qName, prefetchCount, prefetchSize,
		handler}
}

//////////////////////////////////
// Retryable RabbitMQ Consumer //
// Requires a bit of Ops works:
// 1: Topic Exchange=E with Qs
// 2: Each Q has DLX=DLE and DLK but NOT TTL which msg will be move into DLE immediately by calling Nack(requeue:false)
// 3: DL Topic Exchange=DLE with DLQs
// 4: Each DLQ has a TTL and DLX=E with DLK which will redirect msg back to Qs when TTL expired, see x-death in headers
type retryableConsumer struct {
	*consumer
	limit int
}

func newRetryableConsumer(id int, logger logging.Logger, qName string, prefetchCount, prefetchSize int,
	handler MessageHandler, limit int) *retryableConsumer {
	retryableHandler := func() MessageHandler {
		return func(headers amqp.Table, body []byte) error {
			if xDeaths, ok := headers["x-death"]; ok {
				xDeaths, _ := xDeaths.([]interface{})
				xDeath, _ := xDeaths[0].(amqp.Table)
				retried, _ := xDeath["count"].(int64)
				if int(retried) >= limit {
					logger.Warnf("( %d ) reached retry limit ( %d ) drop message", id, limit)
					return nil
				} else {
					return handler.handle(headers, body)
				}
			} else {
				return handler.handle(headers, body)
			}
		}
	}
	return &retryableConsumer{
		newConsumer(id, logger, qName, prefetchCount, prefetchSize, retryableHandler()),
		limit}
}
