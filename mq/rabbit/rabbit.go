package rabbit

import (
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

func Publish(ctx context.Context, qUser, qPassword, qUri, exchange, topic string, body []byte) error {
	qConn := NewConnection(ctx, qUser, qPassword, qUri)
	if _, err := qConn.connect(); err != nil {
		qConn.logger.Error(err)
		return err
	} else {
		if qChan, err := qConn.channel(0, 0); err != nil {
			qConn.logger.Errorf("( %s ) channel failed ( %s )", qUri, err.Error())
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
				qConn.logger.Errorf("publish ( %s ) failed ( %s )", string(body), err.Error())
				return err
			} else {
				qConn.logger.Debugf("publish ( %s )  succeed", string(body))
				return nil
			}
		}
	}
}

// 1 Connection with N Consumers per Queue
func RunConsumer(ctx context.Context, qUser, qPassword, qUri, qName string, prefetchCount, prefetchSize,
	maxRetries, workers int, handler MessageHandler) {
	conn := NewConnection(ctx, qUser, qPassword, qUri)
	for id := 0; id < workers; id++ {
		c := newRetryableConsumer(id, qName, prefetchCount, prefetchSize, handler, maxRetries)
		c.run(conn)
	}
	conn.start()
}

// N Consumers per Queue reusing Connection
func RunConsumerUpon(conn *Connection, qName string, prefetchCount, prefetchSize, maxRetries, workers int,
	handler MessageHandler) {
	for id := 0; id < workers; id++ {
		c := newRetryableConsumer(id, qName, prefetchCount, prefetchSize, handler, maxRetries)
		c.run(conn)
	}
	conn.start()
}
