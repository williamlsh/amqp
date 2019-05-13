package main

import (
	"amqp/consumer"
	"flag"
	"log"
)

func main() {
	var (
		amqpURI      = flag.String("amqpURI", "", "RabbitMQ server uri")
		exchange     = flag.String("exchange", "", "RabbitMQ exchange name")
		exchangeType = flag.String("exchangeType", "topic", "RabbitMQ exchange type")
		queue        = flag.String("queue", "", "RabbitMQ queue name")
		bindingKey   = flag.String("bindingKey", "", "RabbitMQ queue binding key")
		consumerTag  = flag.String("consumerTag", "", "RabbitMQ consumer tag")
	)
	flag.Parse()

	c := consumer.New(*consumerTag)
	err := c.Consume(*amqpURI, *exchange, *exchangeType, *queue, *bindingKey)
	if err != nil {
		log.Fatalf("%s", err)
	}

	for d := range c.Deliveries() {
		log.Printf(
			"got %dB delivery: [%v] %q %q",
			len(d.Body),
			d.DeliveryTag,
			d.RoutingKey,
			d.Body,
		)
		d.Ack(false)
	}
	log.Printf("Handle: deliveries channel closed")
	c.Done() <- nil

	log.Printf("Shutting down")
	if err := c.Shutdown(); err != nil {
		log.Fatalf("error during shutdown: %s", err)
	}
}
