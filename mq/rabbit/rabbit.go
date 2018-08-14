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

func connect(logger logging.Logger, qUser, qPassword, qUri string, reconnect reconnect) (
	qConn *amqp.Connection, qChan *amqp.Channel) {
	var err error = nil
	retry := 0
	for {
		if qConn, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", qUser, qPassword, qUri)); err != nil {
			logger.Errorf("connect failed ( %d, %s )", retry, err.Error())
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
			logger.Errorf("channel create failed ( %d, %s )", retry, err.Error())
			retry++
			time.Sleep(common.RandomDuration(retry))
		} else {
			break
		}
	}

	// multiple consumers , each one is relatively slow due to mysql insert
	// so set count to 30 to give it a go
	if err := qChan.Qos(30, 0, true); err != nil {
		logger.Errorf("channel qos failed ( %s )", err.Error())
	}

	// reconnect when connection dropped
	go func() {
		if err := <-qConn.NotifyClose(make(chan *amqp.Error)); err != nil {
			logger.Errorf("connection dropped ( %s ), reconnecting", err.Error())
			reconnect(qUser, qPassword, qUri)
		}
	}()

	return
}

type MessageHandler func([]byte) error

func (mh MessageHandler) handle(body []byte) error { return mh(body) }
func consume(logger logging.Logger, qChan *amqp.Channel, qName string, handler MessageHandler) error {
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
				logger.Errorf("handle message ( %s ) failed ( %s )", string(m.Body), err.Error())
				if e := m.Nack(false, true); e != nil {
					logger.Errorf("nack ( %s ) failed: %s", string(m.Body), e.Error())
				}
			} else {
				logger.Debugf("handle message ( %s ) succeed", string(m.Body))
				if e := m.Ack(false); e != nil {
					logger.Errorf("ack ( %s ) failed ( %s )", string(m.Body), e.Error())
				}
			}
		}
		return nil
	}
}

func retrieve(logger logging.Logger, qChan *amqp.Channel, qName string, labor model.Labor) error {
	if m, ok, err := qChan.Get(qName, false); ok {
		if r, err := labor.Work(m.Body); err != nil {
			logger.Errorf("handle message ( %s ) failed ( %s )", string(m.Body), err.Error())
			if e := m.Nack(false, true); e != nil {
				logger.Errorf("nack ( %s ) failed: %s", string(m.Body), e.Error())
			}
			return err
		} else {
			logger.Debugf("handle message ( %s ) succeed ( %+v )", string(m.Body), r)
			if e := m.Ack(false); e != nil {
				logger.Errorf("ack ( %s ) failed ( %s )", string(m.Body), e.Error())
				return e
			} else {
				return nil
			}
		}
	} else {
		logger.Errorf("retrieve failed ( %s )", err.Error())
		return err
	}
}
