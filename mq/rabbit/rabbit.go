package rabbit

import (
	"github.com/samwooo/bolsa/logging"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

func Publish(ctx context.Context, qUser, qPassword, qUri, exchange, topic string, body []byte) error {
	logger := logging.GetLogger(" #" + exchange + " " + topic + "# ")
	qConn := NewConnection(ctx, logger, qUser, qPassword, qUri)
	if _, err := qConn.connect(); err != nil {
		logger.Error(err)
		return err
	} else {
		if qChan, err := qConn.channel(0, 0); err != nil {
			logger.Errorf("( %s ) channel failed ( %s )", qUri, err.Error())
			return err
		} else {
			if err := qChan.Publish(
				exchange,
				topic,
				false,
				false,
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        body,
				}); err != nil {
				logger.Errorf("publish ( %s ) failed ( %s )", string(body), err.Error())
				return err
			} else {
				logger.Debugf("publish ( %s )  succeed", string(body))
				return nil
			}
		}
	}
}

// 1 Connection with N Consumers per Queue
func RunConsumer(ctx context.Context, qUser, qPassword, qUri, qName string, prefetchCount, prefetchSize,
	maxRetries, workers int, handler MessageHandler) {
	logger := logging.GetLogger(" #" + qName + "# ")
	conn := NewConnection(ctx, logger, qUser, qPassword, qUri)
	for id := 0; id < workers; id++ {
		c := newRetryableConsumer(id, logger, qName, prefetchCount, prefetchSize, handler, maxRetries)
		c.run(conn)
	}
	conn.start()
}

// N Consumers per Queue reusing Connection
func RunConsumerUpon(conn *Connection, qName string, prefetchCount, prefetchSize, maxRetries, workers int,
	handler MessageHandler) {
	logger := logging.GetLogger(" #" + qName + "# ")
	for id := 0; id < workers; id++ {
		c := newRetryableConsumer(id, logger, qName, prefetchCount, prefetchSize, handler, maxRetries)
		c.run(conn)
	}
	conn.start()
}
