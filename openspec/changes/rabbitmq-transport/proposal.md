# RabbitMQ Transport

## Why

The framework supports only in-JVM (LocalQueue) and Apache Kafka transports.
RabbitMQ is a widely adopted message broker, and supporting it would let
applications integrate with existing RabbitMQ infrastructure using the same
transport contracts as Kafka.

## What Changes

- Add a new transport type `rabbitmq` that implements the existing
  `InTransport` and `OutTransport` contracts
- The incoming transport SHALL subscribe to a RabbitMQ queue and deliver
  deserialized events to the consumer
- The outgoing transport SHALL publish events to a RabbitMQ exchange
- Transports of the same connection SHALL be built together and share
  the broker connection lifecycle
- Spring auto-configuration SHALL support `rabbitmq` as a configured
  transport type via transport factories

## Capabilities

### New Capabilities
- `transports/rabbitmq`: RabbitMQ incoming and outgoing transports,
  connection management, and Spring integration

### Modified Capabilities
<!-- No existing capability requirements change; RabbitMQ is a new transport type. -->

## Impact

- event-flow-core: new RabbitMQ transport implementations
- event-flow-spring: new transport factories and auto-configuration
- Dependencies: RabbitMQ AMQP client library added to both modules
- No existing APIs change; transport contracts are reused as-is

## Non-goals

- No RabbitMQ stream plugin support (only AMQP 0-9-1 queues/exchanges)
- No message acknowledgment customization beyond framework defaults
- No support for RabbitMQ as an event store
