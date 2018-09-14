package rabbit

import (
	"fmt"

	"github.com/samwooo/bolsa/job/model"
	"github.com/samwooo/bolsa/logging"
	"github.com/streadway/amqp"
)

type reconnect func(user, password, uri string)

func connect(logger logging.Logger, qUser, qPassword, qUri string, doReconnect reconnect) (qConn *amqp.Connection,
	qChan *amqp.Channel) {
	var err error = nil
	retry := 0
	for {
		if qConn, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", qUser, qPassword, qUri)); err != nil {
			logger.Errorf("connect failed ( %d, %s )", retry, err.Error())
			retry++
		} else {
			logger.Infof("connection to q %s established.", qUri)
			break
		}
	}
	retry = 0
	for {
		if qChan, err = qConn.Channel(); err != nil {
			logger.Errorf("channel create failed ( %d, %s )", retry, err.Error())
			retry++
		} else {
			logger.Infof("channel to q %s established.", qUri)
			break
		}
	}
	// multiple consumers , each one is relatively slow due to mysql insert
	// so set count to 30 to give it a go
	if err := qChan.Qos(30, 0, true); err != nil {
		logger.Errorf("channel qos failed ( %s )", err.Error())
	}

	go func() {
		if err := <-qConn.NotifyClose(make(chan *amqp.Error)); err != nil {
			logger.Errorf("connection dropped ( %s ), reconnecting", err.Error())
			doReconnect(qUser, qPassword, qUri)
		}
	}()
	return
}

type MessageHandler func(amqp.Table, []byte) error

func (mh MessageHandler) handle(headers amqp.Table, body []byte) error { return mh(headers, body) }
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
			// only with headers we can do TTL DLX ... and from [x-death] then we can implement retry
			if err := handler.handle(m.Headers, m.Body); err != nil {
				logger.Errorf("handle message ( %+v ) ( %s ) failed ( %s )", m.Headers, string(m.Body), err.Error())
				if e := m.Nack(false, false); e != nil {
					logger.Errorf("nack ( %+v ) ( %s ) failed ( %s )", m.Headers, string(m.Body), e.Error())
				}
			} else {
				logger.Debugf("handle message ( %+v ) ( %s ) succeed", m.Headers, string(m.Body))
				if e := m.Ack(false); e != nil {
					logger.Errorf("ack ( %+v ) ( %s ) failed ( %s )", m.Headers, string(m.Body), e.Error())
				}
			}
		}
		return nil
	}
}

func retrieve(logger logging.Logger, qChan *amqp.Channel, qName string, labor model.Labor) error {
	if m, ok, err := qChan.Get(qName, false); ok {
		if r, err := labor.Work(m); err != nil {
			logger.Errorf("handle message ( %+v ) ( %s ) failed ( %s )", m.Headers, string(m.Body), err.Error())
			if e := m.Nack(false, false); e != nil {
				logger.Errorf("nack ( %+v ) ( %s ) failed ( %s )", m.Headers, string(m.Body), e.Error())
			}
			return err
		} else {
			logger.Debugf("handle message ( %+v ) ( %s ) succeed ( %+v )", m.Headers, string(m.Body), r)
			if e := m.Ack(false); e != nil {
				logger.Errorf("ack ( %+v ) ( %s ) failed ( %s )", m.Headers, string(m.Body), e.Error())
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

func publish(logger logging.Logger, qChan *amqp.Channel, exchange, topic string, body []byte) error {
	if err := qChan.Publish(
		exchange,
		topic,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		}); err != nil {
		logger.Errorf("publish ( %s ) to ( %s,%s ) failed ( %s )", string(body), exchange, topic, err.Error())
		return err
	} else {
		logger.Debugf("publish ( %s ) to ( %s,%s ) succeed", string(body), exchange, topic)
		return nil
	}
}
