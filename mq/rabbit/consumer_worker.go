package rabbit

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type consumerWorker struct {
	ctx     context.Context
	cancel  context.CancelFunc
	channel *Channel
	id      int
	name    string
	handler MessageHandler
	logger  logrus.FieldLogger
}

func (w *consumerWorker) Name() string { return fmt.Sprintf("C(%s)(%05d)", w.name, w.id) }
func (w *consumerWorker) consume(msgCh <-chan amqp.Delivery) {
	/*
		Continues deliveries to the returned chan Delivery until Channel.Cancel,
		Connection.Close, Channel.Close, or an AMQP exception occurs.  Consumers must
		range over the chan to ensure all deliveries are received.  Unreceived
		deliveries will block all methods on the same connection.
	*/
	go func() {
		for m := range msgCh {
			w.logger.WithField("&", "Consume").Debug("=> Handle")
			if err := w.handler.handle(m.Headers, m.Body); err != nil {
				w.logger.WithField("&", "Consume").Error("=> Handle failed: ", err)
				w.logger.WithField("&", "Consume").Debug("=> Nack")
				if e := m.Nack(false, false); e != nil {
					w.logger.WithField("&", "Consume").Error("=> Nack failed: ", err)
				}
			} else {
				w.logger.WithField("&", "Consume").Debug("=> Ack")
				if e := m.Ack(false); e != nil {
					w.logger.WithField("&", "Consume").Error("=> Ack failed: ", err)
				}
			}
		}
		w.logger.WithField("&", "Consume").Info("=> Stopped")
	}()
}

func (w *consumerWorker) run() *consumerWorker {
	key := w.Name()
	w.logger.WithField("&", "Start").Debug("=> Register " + key)
	established, terminated, teared := w.channel.Accept(key)
	go func() {
		for {
			select {
			case <-w.ctx.Done():
				w.cancel()
				if w.channel != nil {
					w.logger.WithField("&", "Stop").Debug("=> Cancel")
					if err := w.channel.Cancel(w.Name(), false); err != nil {
						w.logger.WithField("&", "Stop").Error("=> Cancel failed: ", err)
					}
				}
				w.logger.WithField("&", "Stop").Info("=> Teared")
				w.channel.tearDown(teared)
				w.logger.WithField("&", "Stop").Info("=> Stopped")
				return
			case <-terminated:
				/*
					channel terminated which closed msgCh as well,
					just keep going and wait for established.
				*/
				w.logger.WithField("&", "Terminate").Debug("=> Received channel terminated")
				w.logger.WithField("&", "Terminate").Info("=> Teared")
				w.channel.tearDown(teared)
				w.logger.WithField("&", "Terminate").Info("=> Still alive")
			case <-established:
				w.logger.WithField("&", "Run").Info("=> Established")
				w.logger.WithField("&", "Run").Debug("=> Consume")
				if msgCh, err := w.channel.Consume(
					w.name,
					w.Name(),
					false,
					false,
					false,
					false,
					nil); err != nil {
					w.logger.WithField("&", "Run").Error("=> Consume failed: ", err)
				} else {
					w.consume(msgCh)
				}
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
	w.channel.Start()
	return w
}

func newConsumerWorker(
	channel *Channel,
	id int,
	name string,
	handler MessageHandler,
	logger logrus.FieldLogger,
) *consumerWorker {
	cw := &consumerWorker{
		channel: channel,
		id:      id,
		name:    name,
		handler: handler,
	}
	cw.ctx, cw.cancel = context.WithCancel(channel.Context())
	cw.logger = logger.WithField("#", cw.Name())
	return cw
}
