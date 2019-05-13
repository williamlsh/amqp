package producer

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

// Publish takes necessary parameters for a AMQP producer to publish data to AMQP server.
// In this circumstance, exchangeType should be `topic`, reliable should be `true`.
func Publish(amqpURI, exchange, exchangeType, routingKey string, body []byte, reliable bool) error {
	log.Printf("Dialing AMQP %q", amqpURI)
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		return fmt.Errorf("dial: %s", err)
	}
	defer conn.Close()

	log.Printf("Got connection, getting channel")
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %s", err)
	}

	log.Printf("Got channel, declaring %q exchange (%q)", exchangeType, exchange)
	err = ch.ExchangeDeclare(
		exchange,     // name
		exchangeType, // type
		true,         // durable
		false,        // auto-delete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return fmt.Errorf("exchange declare: %s", err)
	}

	if reliable {
		log.Printf("Enabling publishing confirms")
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("channel could not be put into confirm mode: %s", err)
		}

		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

		defer confirmOne(confirms)
	}

	log.Printf("Declared exchange, publishing %dB body (%s)", len(string(body)), body)
	err = ch.Publish(
		exchange,   // publish to an existing exchange
		routingKey, // routing to 0 or more queues
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "utf-8",
			Body:            body,
			DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
			Priority:        0,               // 0-9
		},
	)
	if err != nil {
		return fmt.Errorf("exchange publish: %s", err)
	}

	return nil
}

func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Printf("Waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("Confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Printf("Failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}
