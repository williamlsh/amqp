# AMQP

RabbitMQ user-friendly interfaces.

## Description

AMQP is a general RabbitMQ encapsulated user-friendly interface providing both producer and consumer methods based on `Topics` pattern.

## How to run example

Run amqp producer:

```bash
go run . -amqpURI="[AMQP_URI]" \
  -exchange="exchange_test" \
  -exchangeType="topic" \
  -routingKey="test" \
  -reliable=false \
  -body="{}"
```

Run amqp consumer:

```bash
go run . -amqpURI="[AMQP_URI]" \
  -exchange="exchange_test" \
  -exchangeType="topic" \
  -bindingKey="test"
```

## Configuration

To both producer and consumer:

- `exchangeType` must be `topic`
- `exchange` must be the same
- producer `routingKey` is the same as consumer `bindingKey`
- producer `reliable` is `false`
- producer body is JSON
