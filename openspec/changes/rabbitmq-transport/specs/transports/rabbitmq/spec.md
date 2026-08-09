## Purpose

Adds RabbitMQ (AMQP 0-9-1) as a transport type for event-flow, enabling
incoming and outgoing event delivery through RabbitMQ queues and exchanges
using the same transport contracts as existing transports.

## ADDED Requirements

### Requirement: RabbitMQ Incoming Transport
The system SHALL provide an incoming RabbitMQ transport that subscribes to a
queue and delivers events to the consumer. The transport SHALL support a
configurable queue name and configurable AMQP connection properties. The
transport SHALL consume messages using the configured serializers and SHALL
stop gracefully.

#### Scenario: RabbitMQ delivery
- **WHEN** an incoming RabbitMQ transport is started with a consumer
- **THEN** it SHALL deliver deserialized events from the queue to the consumer

#### Scenario: RabbitMQ stop
- **WHEN** a started incoming RabbitMQ transport is stopped
- **THEN** the consumer SHALL be cancelled and the connection SHALL be released

### Requirement: RabbitMQ Outgoing Transport
The system SHALL provide an outgoing RabbitMQ transport that publishes events
to an exchange. The transport SHALL use the configured serializers to encode
events and SHALL return a send result reporting success or failure. Sending on
a closed transport SHALL fail.

#### Scenario: RabbitMQ publish success
- **WHEN** an outgoing RabbitMQ transport publishes an event
- **THEN** the event SHALL be delivered to the configured exchange
- **AND** the send result SHALL indicate success

#### Scenario: Publish on closed transport
- **WHEN** an event is sent on a closed outgoing RabbitMQ transport
- **THEN** an `IllegalStateException` SHALL be thrown

#### Scenario: RabbitMQ publish failure
- **WHEN** the RabbitMQ broker rejects the publish
- **THEN** the send result SHALL indicate failure with error details

### Requirement: RabbitMQ Transport Pair
The system SHALL provide a builder that constructs a matching pair of incoming
and outgoing RabbitMQ transports sharing one broker connection. The builder
SHALL support configurable connection properties and queue/exchange names.

#### Scenario: Shared connection pair
- **WHEN** a RabbitMQ transport pair is built
- **THEN** the incoming and outgoing transports SHALL share the same connection

#### Scenario: Custom connection properties
- **WHEN** a RabbitMQ transport pair is built with custom connection properties
- **THEN** the transports SHALL use the configured connection settings
