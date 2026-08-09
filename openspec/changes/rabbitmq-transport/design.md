# RabbitMQ Transport Design

## Context

See proposal.md - Why.

The framework already supports two transport types: local-queue (in-JVM,
`LocalQueueInTransport`/`LocalQueueOutTransport` sharing a `BlockingDeque`) and
Kafka (`KafkaInTransport`/`KafkaOutTransport`/`BroadcastKafkaOutTransport`).
Kafka transports establish the house patterns this design reuses:

- `InTransport` starts a consumer loop on an internal executor and delivers
  deserialized events to a consumer callback; `OutTransport.send` returns a
  `CompletableFuture<SendResult>`.
- Serialization goes through `EventSerializer` (magic byte prefix) and
  `EventSerializerFactory`; Kafka incoming uses the factory to pick a
  serializer by magic byte, Kafka outgoing uses a concrete serializer
  (default `JsonEventSerializer`).
- Spring integration is driven by `InTransportFactory` / `OutTransportFactory`
  discovered as `@Component` beans and selected by transport type name.

Architecture constraint (from config.yaml): `event-flow-core` SHALL NOT depend
on Spring Framework; Spring integration lives only in `event-flow-spring`.

## Goals / Non-Goals

**Goals:**
- Add `rabbitmq` transport type in `event-flow-core` following the existing
  transport contracts (`InTransport`/`OutTransport`/`SendResult`).
- Provide a builder that creates an incoming/outgoing pair sharing one broker
  connection.
- Wire `rabbitmq` into Spring auto-configuration through the existing factory
  interfaces so no change to `ChannelConfiguration`/`DispatcherConfiguration`
  core logic is needed.
- Reuse the existing `EventSerializer`/`EventSerializerFactory` for message
  encoding so RabbitMQ messages stay compatible with the framework's
  serialization spec.

**Non-Goals:**
- No RabbitMQ stream plugin / AMQP 1.0 support (AMQP 0-9-1 only).
- No custom acknowledgment modes beyond framework-level defaults
  (auto-ack on successful deserialization+delivery).
- No RabbitMQ as an event store; no management API integration.

## Decisions

### Decision 1: New package under `transport` mirrors Kafka layout
`io.github.vovten.eventflow.transport.rabbitmq` in `event-flow-core` with
`incoming/` and `outgoing/` subpackages, matching the existing
`transport/incoming` and `transport/outgoing` layout.

*Rationale:* consistent with how Kafka and local-queue transports are organized;
discoverable by package scanning.

*Alternatives considered:* a separate top-level module - rejected, RabbitMQ is
one transport among several, not a new module.

### Decision 2: Single connection shared by a transport pair via a builder
`RabbitMqConnectionFactory` (wraps `com.rabbitmq:amqp-client` `ConnectionFactory`)
creates one `Connection`. `RabbitMqTransportsBuilder` constructs
`RabbitMqInTransport` + `RabbitMqOutTransport` sharing that connection, mirroring
`LocalQueueTransportsBuilder` which shares a queue.

*Rationale:* a shared connection is the RabbitMQ best practice (channels are the
cheap unit); the builder mirrors the existing local-queue pair builder pattern.

*Alternatives considered:* each transport opens its own connection - rejected,
wastes connections and complicates lifecycle.

### Decision 3: Outgoing transport publishes to an exchange with a configured routing key
`RabbitMqOutTransport.send` publishes the serialized event body to the
configured exchange with a configurable routing key, returning
`CompletableFuture<SendResult>`. Default exchange `""` (default direct
exchange) is used when none is configured, so the routing key maps to a queue
name - simplest topology for a first release.

*Rationale:* matches the spec scenario "publish to the configured exchange";
the default exchange keeps zero-config usage simple.

*Alternatives considered:* mandatory queue-name publishing only - rejected,
less flexible than exchange+routing key.

### Decision 4: Incoming transport consumes via a channel with auto-ack
`RabbitMqInTransport` opens a channel, declares the configured queue
(durable=false unless configured otherwise), and registers a
`DeliverCallback` that deserializes the body via `EventSerializerFactory`
(selected by magic byte) and forwards to the consumer callback. Start is
idempotent; stop cancels the consumer and closes the channel. Connection
release happens when both transports in the pair are stopped (reference
counted by the builder/shared connection owner).

*Rationale:* reuses `EventSerializerFactory` exactly like `KafkaInTransport`;
auto-ack matches the non-goal of avoiding custom ack modes.

*Alternatives considered:* manual ack - rejected (out of scope per non-goals).

### Decision 5: Spring factories for `rabbitmq` transport type
`RabbitMqInTransportFactory` (implements `InTransportFactory`) and
`RabbitMqOutTransportFactory` (implements `OutTransportFactory`) annotated
`@Component` in `event-flow-spring`, returning type/name `"rabbitmq"`. They
read the `EventFlowProperties.TransportConfig` for connection properties
(host, port, virtual-host, username, password) and queue/exchange/routing-key
settings, and reuse the core `RabbitMqTransportsBuilder` so a dispatcher
transport and a publisher transport on the same name share one connection.

*Rationale:* zero changes to `ChannelConfiguration`/`DispatcherConfiguration`;
factories are the extension point already designed for new transports.

*Alternatives considered:* a dedicated RabbitMQ configuration class that
intercepts dispatch/channel creation - rejected, duplicates the existing
factory mechanism.

## Risks / Trade-offs

- [RabbitMQ connection loss / broker restart] → expose connection recovery
  through `amqp-client` recovery settings; document that automatic recovery is
  enabled by default and reconnects with backoff.
- [Serialization mismatch between producer and consumer] → magic-byte based
  `EventSerializerFactory` detection, same as Kafka; unknown magic bytes fail
  fast with a logged error.
- [Shared connection lifecycle (stop order)] → reference-counted close in the
  builder-owned connection holder; double-stop is idempotent.
- [New external dependency `com.rabbitmq:amqp-client`] → kept as a small,
  widely-used client; no transitive Spring coupling in `event-flow-core`.
- [Exchange/routing-key misconfiguration] → `RabbitMqOutTransport` fails fast
  (returns failed `SendResult`) on publish errors; validation in Spring
  factories rejects obviously invalid config at startup.
