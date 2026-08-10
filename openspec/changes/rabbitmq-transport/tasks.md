# RabbitMQ Transport — Tasks

## 1. Dependencies

- [x] 1.1 Add `com.rabbitmq:amqp-client` dependency to event-flow-core `pom.xml`
- [x] 1.2 Add `com.rabbitmq:amqp-client` dependency to event-flow-spring `pom.xml`

## 2. Core: Connection management

- [x] 2.1 Implement `RabbitMqConnectionFactory` in
  `transport/rabbitmq` package wrapping `com.rabbitmq:amqp-client` `ConnectionFactory`
- [x] 2.2 Implement `RabbitMqConnectionHolder` owning the shared `Connection` with
  reference counting so the connection closes when both transports in a pair stop

## 3. Core: Incoming transport

- [x] 3.1 Implement `RabbitMqInTransport` implementing `InTransport` in
  `transport/rabbitmq/incoming`
- [x] 3.2 Declare the configured queue (durable=false by default) on start
- [x] 3.3 Register a `DeliverCallback` that deserializes the body via
  `EventSerializerFactory` (magic-byte detection) and forwards to the consumer
- [x] 3.4 Make `start` idempotent and `stop` cancel the consumer and close the channel

## 4. Core: Outgoing transport

- [x] 4.1 Implement `RabbitMqOutTransport` implementing `OutTransport` in
  `transport/rabbitmq/outgoing`
- [x] 4.2 Implement `send` publishing the serialized event to the configured exchange
  (default `""`) with a routing key, returning `CompletableFuture<SendResult>`
- [x] 4.3 Throw `IllegalStateException` when sending on a closed transport

## 5. Core: Builder

- [x] 5.1 Implement `RabbitMqTransportsBuilder` that constructs
  `RabbitMqInTransport` + `RabbitMqOutTransport` sharing one connection
- [x] 5.2 Support configurable connection properties and queue/exchange/routing-key
  settings in the builder

## 6. Core: Tests

- [x] 6.1 Write tests for `RabbitMqInTransport` covering the spec scenarios
  (delivery, stop) using an in-process broker or mocked channel
- [x] 6.2 Write tests for `RabbitMqOutTransport` covering publish success,
  closed transport, and publish failure scenarios
- [x] 6.3 Write tests for `RabbitMqTransportsBuilder` covering shared connection
  and custom connection properties
- [x] 6.4 Ensure all tests pass with `mvn test` (event-flow-core)

## 7. Spring: Auto-configuration

- [x] 7.1 Implement `RabbitMqInTransportFactory` implementing `InTransportFactory`
  returning type `rabbitmq` in `event-flow-spring`
- [x] 7.2 Implement `RabbitMqOutTransportFactory` implementing `OutTransportFactory`
  returning name `rabbitmq` in `event-flow-spring`
- [x] 7.3 Map `EventFlowProperties.TransportConfig` to builder settings
  (host, port, virtual-host, username, password, queue, exchange, routing-key)
- [x] 7.4 Write Spring context tests verifying `rabbitmq` transport wiring
- [x] 7.5 Ensure all tests pass with `mvn test` (event-flow-spring)

## 8. Example configuration

- [x] 8.1 Add a `rabbitmq` transport example to
  `event-flow-spring/src/main/resources/event-flow.yml` for the publisher channel
- [x] 8.2 Add a `rabbitmq` transport example to
  `event-flow-spring/src/main/resources/event-flow.yml` for the dispatcher transports
