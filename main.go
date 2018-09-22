package main

import (
	"context"
	"fmt"

	"github.com/samwooo/bolsa/logging"
	"github.com/samwooo/bolsa/mq/rabbit"
	"github.com/streadway/amqp"
)

func main() {
	logging.DefaultLogger("\t", logging.INFO, 100)

	rabbit.RunConsumer(context.Background(), "admin", "admin", "127.0.0.1:5672",
		"cardLeagueTax", 30, 0, 3000, 4,
		func(headers amqp.Table, body []byte) error {
			return fmt.Errorf("not null")
		})

	conn := rabbit.NewConnection(context.Background(), logging.GetLogger(""), "admin", "admin",
		"127.0.0.1:5672")
	rabbit.RunConsumerUpon(conn, "cardLeagueTax", 30, 0, 3000, 4,
		func(headers amqp.Table, body []byte) error {
			return fmt.Errorf("not null")
		})

	fmt.Scanln()
}
