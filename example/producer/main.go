package main

import (
	"amqp/producer"
	"flag"
	"log"
	"time"
)

func main() {
	var (
		amqpURI      = flag.String("amqpURI", "", "RabbitMQ server uri")
		exchange     = flag.String("exchange", "", "RabbitMQ exchange name")
		exchangeType = flag.String("exchangeType", "topic", "RabbitMQ exchange type")
		routingKey   = flag.String("routingKey", "", "RabbitMQ queue routing key")
		body         = flag.String("body", `{}`, "message body")
		reliable     = flag.Bool("reliable", false, "RabbitMQ message confirmation")
	)
	flag.Parse()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := publish(*amqpURI, *exchange, *exchangeType, *routingKey, []byte(*body), *reliable)
			if err != nil {
				break
			}
		}
	}
}

func publish(amqpURI, exchange, exchangeType, routingKey string, body []byte, reliable bool) error {
	if err := producer.Publish(amqpURI, exchange, exchangeType, routingKey, body, reliable); err != nil {
		log.Printf("publish error: %s", err)
		return err
	}
	log.Printf("published %dB OK", len(body))
	return nil
}
