package rabbit

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Channel struct {
	*medium
	*Connection
	*amqp.Channel
	id                   int
	prefetchCount        int
	prefetchSize         int
	connectionTerminated chan struct{}
}

func (c *Channel) Name() string { return fmt.Sprintf("🔗(%05d)", c.id) }
func (c *Channel) Stop()        {}

func (c *Channel) establishAndMonitorChannel(teared chan<- struct{}) {
	c.logger.WithField("&", "Start").Debug("=> Channel")
	if c.Channel = c.Connection.Channel(c.prefetchCount, c.prefetchSize); c.Channel != nil {
		c.logger.WithField("&", "Start").Debug("=> NotifyEstablished")
		c.Notify(&c.established)

		go func() {
			if err := <-c.Channel.NotifyClose(make(chan *amqp.Error, 1)); err != nil {
				c.logger.WithField("&", "Monitor").Error("=> Dropped: ", err)
				c.logger.WithField("&", "Monitor").Debug("=> NotifyTerminated")
				c.Notify(&c.terminated)
				c.logger.WithField("&", "Monitor").Debug("=> Await")
				c.Await()
				/*
					publish to non-existing exchange can also kill the channel but not the connection
				*/
				if c.IsClosed() {
					/*
						wait for connection to be terminated
					*/
					c.logger.WithField("&", "Monitor").Debug("=> Wait connection terminated")
					<-c.connectionTerminated
					/*
						restart to listen on events first
					*/
					c.logger.WithField("&", "Monitor").Info("=> Restart")
					c.Start()
					/*
						tell connection you may restart
					*/
					c.logger.WithField("&", "Monitor").Info("=> Teared")
					c.tearDown(teared)
				} else {
					/*
						connection is alive just refresh channel and monitor it
					*/
					c.establishAndMonitorChannel(teared)
				}
			}
		}()
	}
}

func (c *Channel) Start() {
	key := c.Name()
	c.logger.WithField("&", "Start").Debug("=> Register " + key)
	established, terminated, teared := c.Connection.Accept(key)
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				c.cancel()
				c.logger.WithField("&", "Stop").Debug("=> Await")
				c.Await()
				c.logger.WithField("&", "Stop").Debug("=> Close")
				if err := c.Channel.Close(); err != nil {
					c.logger.WithField("&", "Stop").Error("=> Close failed: ", err)
				}
				c.logger.WithField("&", "Stop").Info("=> Clean")
				c.Clean()
				c.logger.WithField("&", "Stop").Info("=> Teared")
				c.tearDown(teared)
				c.logger.WithField("&", "Stop").Info("=> Stopped")
				return
			case <-terminated:
				c.logger.WithField("&", "Terminate").Debug("=> Received connection terminated")
				c.connectionTerminated <- struct{}{}
				c.logger.WithField("&", "Terminate").Debug("=> Terminated")
				return
			case <-established:
				c.logger.WithField("&", "Start").Info("=> Established")
				c.establishAndMonitorChannel(teared)
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
}

func NewChannel(
	id int,
	connection *Connection,
	prefetchCount, prefetchSize int,
	logger logrus.FieldLogger,
) *Channel {
	c := &Channel{
		Channel:              nil,
		Connection:           connection,
		id:                   id,
		prefetchCount:        prefetchCount,
		prefetchSize:         prefetchSize,
		connectionTerminated: make(chan struct{}, 1),
	}
	c.medium = NewMedium(connection.Context(), logger.WithField("#", c.Name()))
	return c
}
