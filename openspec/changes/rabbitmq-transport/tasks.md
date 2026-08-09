# RabbitMQ Transport — Tasks

## 1. Dependencies

- [ ] 1.1 Add `com.rabbitmq:amqp-client` dependency to event-flow-core `pom.xml`
- [ ] 1.2 Add `com.rabbitmq:amqp-client` dependency to event-flow-spring `pom.xml`

## 2. Core: Connection management

- [ ] 2.1 Implement `RabbitMqConnectionFactory` in
  `transport/rabbitmq` package wrapping `com.rabbitmq:amqp-client` `ConnectionFactory`
- [ ] 2.2 Implement `RabbitMqConnectionHolder` owning the shared `Connection` with
  reference counting so the connection closes when both transports in a pair stop

## 3. Core: Incoming transport

- [ ] 3.1 Implement `RabbitMqInTransport` implementing `InTransport` in
  `transport/rabbitmq/incoming`
- [ ] 3.2 Declare the configured queue (durable=false by default) on start
- [ ] 3.3 Register a `DeliverCallback` that deserializes the body via
  `EventSerializerFactory` (magic-byte detection) and forwards to the consumer
- [ ] 3.4 Make `start` idempotent and `stop` cancel the consumer and close the channel

## 4. Core: Outgoing transport

- [ ] 4.1 Implement `RabbitMqOutTransport` implementing `OutTransport` in
  `transport/rabbitmq/outgoing`
- [ ] 4.2 Implement `send` publishing the serialized event to the configured exchange
  (default `""`) with a routing key, returning `CompletableFuture<SendResult>`
- [ ] 4.3 Throw `IllegalStateException` when sending on a closed transport

## 5. Core: Builder

- [ ] 5.1 Implement `RabbitMqTransportsBuilder` that constructs
  `RabbitMqInTransport` + `RabbitMqOutTransport` sharing one connection
- [ ] 5.2 Support configurable connection properties and queue/exchange/routing-key
  settings in the builder

## 6. Core: Tests

- [ ] 6.1 Write tests for `RabbitMqInTransport` covering the spec scenarios
  (delivery, stop) using an in-process broker or mocked channel
- [ ] 6.2 Write tests for `RabbitMqOutTransport` covering publish success,
  closed transport, and publish failure scenarios
- [ ] 6.3 Write tests for `RabbitMqTransportsBuilder` covering shared connection
  and custom connection properties
- [ ] 6.4 Ensure all tests pass with `mvn test` (event-flow-core)

## 7. Spring: Auto-configuration

- [ ] 7.1 Implement `RabbitMqInTransportFactory` implementing `InTransportFactory`
  returning type `rabbitmq` in `event-flow-spring`
- [ ] 7.2 Implement `RabbitMqOutTransportFactory` implementing `OutTransportFactory`
  returning name `rabbitmq` in `event-flow-spring`
- [ ] 7.3 Map `EventFlowProperties.TransportConfig` to builder settings
  (host, port, virtual-host, username, password, queue, exchange, routing-key)
- [ ] 7.4 Write Spring context tests verifying `rabbitmq` transport wiring
- [ ] 7.5 Ensure all tests pass with `mvn test` (event-flow-spring)
