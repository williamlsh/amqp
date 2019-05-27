package consumer

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

// Consumer represents AMQP queue consumer.
type Consumer interface {
	// Consume sets up a RabbitMQ consumer including net connection and topics subscribing.
	Consume(amqpURI, exchange, exchangeType, queueName, bindingKey string) error
	// Deliveries returns a receiving only channel containing amqp Deliveries.
	// The Deliveries carry source data messages and are ready to consume.
	Deliveries() <-chan amqp.Delivery
	// Done returns a signal channel for sending when consuming is done.
	Done() chan error
	// Shutdown closes the consumer's deliveries channel and net connection.
	Shutdown() error
}

// consumer implements the Consumer interface.
type consumer struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	tag        string
	deliveries <-chan amqp.Delivery
	done       chan error
}

// New returns a new consumer which implements Consumer interface.
// In topic exchange circumstance, exchangeType should be `topic`, exchange
// should be the same with producer's exchange, bindingKey should be the same
// with producer's routingkey, queuename and ctag should be all empty strings.
func New(ctag string) Consumer {
	c := &consumer{
		conn:       nil,
		channel:    nil,
		tag:        ctag,
		deliveries: nil,
		done:       make(chan error),
	}
	return c
}

// Consume sets up a RabbitMQ consumer including net connection and topics
// subscribing. It instantiates consumer with deliveries channel for data
// processing usage.
//
// Generally, processing function will range this channel until channel closes
// or error occurs during data processing.
func (c *consumer) Consume(amqpURI, exchange, exchangeType, queueName, bindingKey string) error {
	var err error

	log.Printf("Dialing AMQP %q", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return fmt.Errorf("dial: %s", err)
	}

	go func() {
		fmt.Printf("Closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("Got connection, getting channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %s", err)
	}

	log.Printf("Got channel, declaring exchange (%q)", exchange)
	err = c.channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return fmt.Errorf("exchange declare: %s", err)
	}

	log.Printf("Declared exchange, declaring queue %q", queueName)
	queue, err := c.channel.QueueDeclare(
		queueName, // name of the queue
		false,     // durable
		false,     // delete when unused
		true,      // exclusive: When the connection that declared it closes, the queue will be deleted
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("queue declare: %s", err)
	}

	log.Printf("Declared queue (%q %d messages, %d consumers), binding to exchange (key %q)", queue.Name, queue.Messages, queue.Consumers, bindingKey)
	err = c.channel.QueueBind(
		queue.Name, // name
		bindingKey, // binding key
		exchange,   // source exchange
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return fmt.Errorf("queue bind: %s", err)
	}

	// Fair dispatch.
	log.Printf("Queue bound to exchange, setting channel QoS with 1 prefetch count")
	err = c.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)

	log.Printf("Channel QoS is set up, starting consumer (consumer tag %q)", c.tag)
	deliveries, err := c.channel.Consume(
		queue.Name, // name
		c.tag,      // consumerTag,
		false,      // autoAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return fmt.Errorf("queue consume: %s", err)
	}

	c.deliveries = deliveries

	return nil
}

// Deliveries returns a receiving only channel containing amqp Deliveries.
// The Deliveries carry source data messages and are ready to consume.
func (c *consumer) Deliveries() <-chan amqp.Delivery {
	return c.deliveries
}

// Done returns a signal channel for sending when consuming is done.
func (c *consumer) Done() chan error {
	return c.done
}

// Shutdown closes the consumer's deliveries channel and net connection.
func (c *consumer) Shutdown() error {
	// Will close the deliveries channel.
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// Wait for handle() to exit.
	return <-c.done
}
