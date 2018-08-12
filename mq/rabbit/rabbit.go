package rabbit

import (
	"fmt"
	"time"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/job/model"
	"github.com/samwooo/bolsa/logging"
	"github.com/streadway/amqp"
)

type reconnect func(user, password, uri string)

func connect(logger logging.Logger, user, password, uri string, reconnect reconnect) (
	qConn *amqp.Connection, qChan *amqp.Channel) {
	var err error = nil
	retry := 0
	for {
		if qConn, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", user, password, uri)); err != nil {
			logger.Errorf("connect q failed ( %d, %s )", retry, err.Error())
			retry++
			time.Sleep(common.RandomDuration(retry))
		} else {
			break
		}
	}

	retry = 0
	for {
		var err error = nil
		if qChan, err = qConn.Channel(); err != nil {
			logger.Errorf("create channel failed ( %d, %s )", retry, err.Error())
			retry++
			time.Sleep(common.RandomDuration(retry))
		} else {
			break
		}
	}

	// multiple consumers , each one is relatively slow due to mysql insert
	// so set count to 30 to give it a go
	if err := qChan.Qos(30, 0, true); err != nil {
		logger.Errorf("qos channel failed ( %s )", err.Error())
	}

	// reconnect when connection dropped
	go func() {
		if err := <-qConn.NotifyClose(make(chan *amqp.Error)); err != nil {
			logger.Errorf("q connection dropped ( %s ), reconnecting", err.Error())
			reconnect(user, password, uri)
		}
	}()

	return
}

type MessageHandler func([]byte) error

func (mh MessageHandler) handle(body []byte) error { return mh(body) }
func consume(qChan *amqp.Channel, qName string, handler MessageHandler) error {
	if msgCh, err := qChan.Consume(qName,
		"",
		false,
		false,
		false,
		false, nil); err != nil {
		return err
	} else {
		for m := range msgCh {
			if err := handler.handle(m.Body); err != nil {
				m.Nack(false, true)
			} else {
				m.Ack(false)
			}
		}
		return nil
	}
}

func retrieve(qChan *amqp.Channel, qName string, labor model.Labor) error {
	if m, ok, err := qChan.Get(qName, false); ok {
		if _, err := labor.Work(m.Body); err != nil {
			m.Nack(false, true)
			return err
		} else {
			m.Ack(false)
			return nil
		}
	} else {
		return err
	}
}
