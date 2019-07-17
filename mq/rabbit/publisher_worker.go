package rabbit

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type publisherWorker struct {
	ctx     context.Context
	cancel  context.CancelFunc
	channel *Channel
	id      int
	logger  logrus.FieldLogger
}

type content struct {
	exchange string
	topic    string
	data     []byte
}

func (w *publisherWorker) Name() string { return fmt.Sprintf("P(%05d)", w.id) }
func (w *publisherWorker) doPublish(contentCh <-chan content) {
	key := w.Name() + "_publish"
	w.logger.WithField("&", "Publish").Debug("=> Register " + key)
	_, terminated, teared := w.channel.Accept(key)
	go func() {
		for {
			select {
			case <-w.ctx.Done():
				w.logger.WithField("&", "PublishStop").Info("=> Teared")
				w.channel.tearDown(teared)
				w.logger.WithField("&", "PublishStop").Info("=> Stopped")
				return
			case <-terminated:
				/*
					channel terminated, then stop publishing,
					wait for a complete fresh restart(established),
					everything should be still in contentCh.
				*/
				w.logger.WithField("&", "PublishTerminate").Debug("=> Received channel terminated")
				w.logger.WithField("&", "PublishTerminate").Info("=> Teared")
				w.channel.tearDown(teared)
				/*
					remove listener from channel
				*/
				w.logger.WithField("&", "PublishTerminate").Debug("=> Deregister " + key)
				w.channel.Remove(key)
				w.logger.WithField("&", "PublishTerminate").Info("=> Terminated")
				return
			case c := <-contentCh:
				logBody := fmt.Sprintf("(%s,%s)(%s)", c.exchange, c.topic, strings.Replace(
					string(c.data), "\"", "", -1))
				if err := w.channel.Publish(
					c.exchange,
					c.topic,
					true,
					false,
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        c.data,
					}); err != nil {
					w.logger.WithFields(logrus.Fields{
						"&": "Publish",
						"*": logBody,
					}).Error("=> Publish failed:", err)
				} else {
					w.logger.WithFields(logrus.Fields{
						"&": "Publish",
					}).Info("=> Published: ", logBody)
				}
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
}

func (w *publisherWorker) doConfirm(confirmCh chan amqp.Confirmation, returnCh chan amqp.Return) {
	key := w.Name() + "_confirm"
	w.logger.WithField("&", "Confirm").Debug("=> Register " + key)
	_, terminated, teared := w.channel.Accept(key)
	handleConfirmed, handleReturned :=
		func(confirmed amqp.Confirmation) bool {
			if 0 == confirmed.DeliveryTag {
				w.logger.WithField("&", "Confirm").Warn("=> Confirmed: 0 ?")
			} else {
				w.logger.WithField("&", "Confirm").Debug("=> Confirmed: ",
					fmt.Sprintf("%05d", confirmed.DeliveryTag))
			}
			return 0 != confirmed.DeliveryTag
		},
		func(returned amqp.Return) bool {
			w.logger.WithFields(logrus.Fields{
				"&": "Confirm",
				"*": fmt.Sprintf("(%s,%s)(%d:%s)", returned.Exchange, returned.RoutingKey,
					returned.ReplyCode, returned.ReplyText),
			}).Error("=> Returned: ", strings.Replace(string(returned.Body), "\"", "", -1))
			return 0 != returned.ReplyCode
		}
	/*
		The listener chan will be closed when the Channel is closed.

		The capacity of the chan Confirmation must be at least as large as the
		number of outstanding publishings.  Not having enough buffered chans will
		create a deadlock if you attempt to perform other operations on the Connection
		or Channel while confirms are in-flight.

		It's advisable to wait for all Confirmations to arrive before calling
		Channel.Close() or Connection.Close().
	*/

	go func() {
		for {
			select {
			case <-w.ctx.Done():
				w.logger.WithField("&", "ConfirmStop").Info("=> Teared")
				w.channel.tearDown(teared)
				w.logger.WithField("&", "ConfirmStop").Info("=> Stopped")
				return
			case <-terminated:
				/*
					channel terminated, then stop confirming,
					wait for a complete fresh restart(established),
					everything should be still in contentCh.
				*/
				w.logger.WithField("&", "ConfirmTerminate").Debug("=> Received channel terminated")
				w.logger.WithField("&", "ConfirmTerminate").Info("=> Teared")
				w.channel.tearDown(teared)
				w.logger.WithField("&", "ConfirmTerminate").Debug("=> Deregister " + key)
				w.channel.Remove(key)
				w.logger.WithField("&", "ConfirmTerminate").Info("=> Terminated")
				return
			case confirmed := <-confirmCh:
				handleConfirmed(confirmed)
			case returned := <-returnCh:
				handleReturned(returned)
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
}

func (w *publisherWorker) run(contentCh chan content) *publisherWorker {
	key := w.Name()
	w.logger.WithField("&", "Run").Debug("=> Register " + key)
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
					channel terminated,
					just keep going and wait for established for a fresh restart.
				*/
				w.logger.WithField("&", "Terminate").Debug("=> Received channel terminated")
				w.logger.WithField("&", "Terminate").Info("=> Teared")
				w.channel.tearDown(teared)
				w.logger.WithField("&", "Terminate").Debug("=> Still alive")
			case <-established:
				w.logger.WithField("&", "Run").Info("=> Established")
				//w.logger.WithField("&", "Run").Debug(" => Confirm")
				//if err := w.channel.Confirm(false); err != nil {
				//	w.logger.WithField("&", "Run").Error("=> Confirm failed: ", err)
				//} else {
				{
					w.logger.WithField("&", "Run").Debug(" => DoPublish")
					w.doPublish(contentCh)
					// TODO: confirm mode doesn't work, ctrl+c will cause channel.publish hang
					//w.logger.WithField("&", "Run").Debug(" => DoConfirm")
					//w.doConfirm(
					//	w.channel.NotifyPublish(make(chan amqp.Confirmation, 1)),
					//	w.channel.NotifyReturn(make(chan amqp.Return, 1)),
					//)
				}
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
	w.channel.Start()
	return w
}

func newPublisherWorker(
	channel *Channel,
	id int,
	logger logrus.FieldLogger,
) *publisherWorker {
	pw := &publisherWorker{
		channel: channel,
		id:      id,
	}
	pw.ctx, pw.cancel = context.WithCancel(channel.Context())
	pw.logger = logger.WithField("#", pw.Name())
	return pw
}
