package rabbit

import (
	"fmt"
	"os"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/logging"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

type observer interface{ run(*Connection) }

type Connection struct {
	ctx                          context.Context
	logger                       logging.Logger
	qConn                        *amqp.Connection
	qUser, qPassword, qUri       string
	ready                        chan struct{}
	connected, reconnect, closed []chan struct{}
}

func (c *Connection) notify(events []chan struct{}) {
	for _, event := range events {
		event <- struct{}{}
	}
}

func (c *Connection) connect() (*amqp.Connection, error) {
	for retry := 0; retry < 3; retry++ {
		if qConn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", c.qUser, c.qPassword, c.qUri)); err != nil {
			c.logger.Errorf("( %s ) connection failed ( %d, %s )", c.qUri, retry, err.Error())
		} else {
			c.logger.Debugf("( %s ) connection established", c.qUri)
			return qConn, nil
		}
	}
	return nil, fmt.Errorf("( %s ) connection failed", c.qUri)
}

func (c *Connection) start() {
	if qConn, err := c.connect(); err != nil {
		c.logger.Error(err)
	} else {
		c.qConn = qConn
		go func() {
			<-c.ready
			c.notify(c.connected)
			if err := <-c.qConn.NotifyClose(make(chan *amqp.Error)); err != nil {
				c.logger.Errorf("( %s ) connection dropped ( %s ), reconnecting", c.qUri, err.Error())
				c.notify(c.reconnect)
				c.start()
			} else {
				c.notify(c.closed)
				c.cleanup()
			}
		}()
		c.ready <- struct{}{}
	}
}

func (c *Connection) cleanup() {
	clear := func(events []chan struct{}) {
		for _, event := range events {
			close(event)
		}
	}
	clear(c.connected)
	clear(c.reconnect)
	clear(c.closed)
	close(c.ready)
}

func (c *Connection) stop() {
	if c.qConn != nil {
		c.qConn.Close()
	}
}

func (c *Connection) channel(prefetchCount, prefetchSize int) (*amqp.Channel, error) {
	for retry := 0; retry < 3; retry++ {
		if qChan, err := c.qConn.Channel(); err != nil {
			c.logger.Errorf("( %s ) channel failed ( %d, %s )", c.qUri, retry, err.Error())
		} else {
			c.logger.Debugf("( %s ) channel established", c.qUri)
			if err := qChan.Qos(prefetchCount, prefetchSize, true); err != nil {
				return nil, fmt.Errorf("( %s ) channel qos failed ( %s )", c.qUri, err.Error())
			} else {
				return qChan, nil
			}
			break
		}
	}
	return nil, fmt.Errorf("( %s ) channel failed", c.qUri)
}

func (c *Connection) register(observer) (connected, reconnect, closed chan struct{}) {
	connected, reconnect, closed = make(chan struct{}), make(chan struct{}), make(chan struct{})
	c.connected = append(c.connected, connected)
	c.reconnect = append(c.reconnect, reconnect)
	c.closed = append(c.closed, closed)
	return
}

func NewConnection(ctx context.Context, logger logging.Logger, qUser, qPassword, qUri string) *Connection {
	c := &Connection{
		ctx:       ctx,
		logger:    logger,
		qConn:     nil,
		qUser:     qUser,
		qPassword: qPassword,
		qUri:      qUri,
		ready:     make(chan struct{})}

	common.TerminateIf(c.ctx,
		func() {
			c.stop()
			logger.Infof("cancellation, ( %s ) connection closed", c.qUri)
		},
		func(s os.Signal) {
			logger.Infof("signal ( %+v ), ( %s ) connection closed", s, c.qUri)
			c.stop()
		})
	return c
}
