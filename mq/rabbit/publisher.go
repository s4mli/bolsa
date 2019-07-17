package rabbit

import (
	"encoding/json"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

type publisher struct {
	connection *Connection
	contentCh  chan content
	workers    map[int]*publisherWorker
}

func (p *publisher) Publish(exchange, topic string, data []byte) {
	p.contentCh <- content{exchange, topic, data}
}

func (p *publisher) PublishJson(exchange, topic string, data interface{}) error {
	if body, err := json.Marshal(data); err != nil {
		return err
	} else {
		p.Publish(exchange, topic, body)
		return nil
	}
}

func RunPublisher(
	ctx context.Context,
	uri, user, password string,
	prefetchCount, prefetchSize, count int,
	logger logrus.FieldLogger,
) Publisher {
	connection, contentCh := NewConnection(ctx, uri, user, password, logger), make(chan content, count)
	p := &publisher{
		connection: connection,
		contentCh:  contentCh,
		workers: func() map[int]*publisherWorker {
			workers := make(map[int]*publisherWorker, count)
			for id := 0; id < count; id++ {
				workers[id] = newPublisherWorker(
					NewChannel(id+1, connection, prefetchCount, prefetchSize, logger),
					id+1, logger,
				).run(contentCh)
			}
			return workers
		}(),
	}
	connection.Start()
	return p
}
