package rabbit

import (
	"fmt"

	"github.com/s4mli/bolsa/cleaner"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

type Connection struct {
	*medium
	*amqp.Connection
	uri, user, password string
}

func (c *Connection) Name() string { return fmt.Sprintf("⚡(%s)", c.user) }
func (c *Connection) Stop() {
	c.cancel()
	c.logger.WithField("&", "Stop").Debug("=> Await")
	c.Await()
	c.logger.WithField("&", "Stop").Debug("=> Close")
	if err := c.Close(); err != nil {
		c.logger.WithField("&", "Stop").Error("=> Close failed: ", err)
	}
	c.logger.WithField("&", "Stop").Info("=> Clean")
	c.Clean()
	c.logger.WithField("&", "Stop").Info("=> Stopped")
}

func (c *Connection) Start() {
	connect := func() *amqp.Connection {
		if conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/",
			c.user, c.password, c.uri)); err != nil {
			c.logger.WithField("&", "dial").Error(err)
			return nil
		} else {
			return conn
		}
	}

	c.logger.WithField("&", "Start").Debug("=> Connect")
	if conn := connect(); conn != nil {
		c.Connection = conn
		c.logger.WithField("&", "Start").Debug("=> NotifyEstablished")
		c.Notify(&c.established)

		go func() {
			if err := <-c.NotifyClose(make(chan *amqp.Error, 1)); err != nil {
				c.logger.WithField("&", "Monitor").Error("=> Dropped: ", err)
				c.logger.WithField("&", "Monitor").Debug("=> NotifyTerminated")
				c.Notify(&c.terminated)
				c.logger.WithField("&", "Monitor").Debug("=> Await")
				c.Await()
				c.logger.WithField("&", "Monitor").Info("=> Restart")
				c.Start()
			}
		}()
	} else {
		panic(fmt.Errorf("unable to establish connection"))
	}
}

func (c *Connection) Channel(prefetchCount, prefetchSize int) *amqp.Channel {
	if qChan, err := c.Connection.Channel(); err != nil {
		c.logger.WithField("&", "Channel").Error("=> Channel failed: ", err)
		return nil
	} else {
		if err := qChan.Qos(prefetchCount, prefetchSize, true); err != nil {
			c.logger.WithField("&", "Channel").Error("=> Qos failed: ", err)
			return nil
		} else {
			return qChan
		}
	}
}

func NewConnection(
	ctx context.Context,
	uri, user, password string,
	logger logrus.FieldLogger,
) *Connection {
	c := &Connection{
		Connection: nil,
		uri:        uri,
		user:       user,
		password:   password,
	}
	c.medium = NewMedium(ctx, logger.WithField("#", c.Name()))
	cleaner.Register(c)
	return c
}
