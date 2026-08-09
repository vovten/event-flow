# Transports Specification

## Purpose

Transports connect the framework to delivery sources and destinations. Incoming transports deliver events from a source to the dispatcher; outgoing transports deliver events from channels to destinations. This spec defines the transport contracts, the in-JVM local queue transport, the Kafka transports, and the local queue provider.

## Requirements

### Requirement: Transport Contracts
An incoming transport SHALL expose a name, SHALL start consuming events and delivering them to a consumer callback, and SHALL stop gracefully. An outgoing transport SHALL expose a name and SHALL send an event asynchronously, returning a future of a send result.

#### Scenario: Incoming transport lifecycle
- GIVEN an incoming transport
- WHEN start is invoked with a consumer callback
- THEN the transport SHALL begin delivering events to the callback
- AND a subsequent stop SHALL release transport resources

#### Scenario: Outgoing transport send
- GIVEN an outgoing transport
- WHEN an event is sent
- THEN a `CompletableFuture<SendResult>` SHALL be returned

### Requirement: Send Result Reporting
A send result SHALL record success or failure, the destination, a timestamp, an optional message identifier, optional metadata, and error details. A send result collection SHALL support outcome classification: all-success, all-failure, and partial-success.

#### Scenario: Successful send
- GIVEN a successful transport send
- WHEN the send result is inspected
- THEN the result SHALL indicate success
- AND the destination SHALL be populated
- AND a timestamp SHALL be set

#### Scenario: Failed send
- GIVEN a failed transport send
- WHEN the send result is inspected
- THEN the result SHALL indicate failure
- AND the error details SHALL be populated

### Requirement: Local Queue Provider
The system SHALL provide a local queue provider that returns a bounded blocking queue per transport name. Queues SHALL be created on demand and SHALL be shared between the publisher-side and dispatcher-side local queue transports.

#### Scenario: Queue per transport name
- GIVEN a local queue provider
- WHEN a queue is requested for a transport name
- THEN a queue SHALL be returned
- AND a second request for the same name SHALL return the same queue instance

#### Scenario: Queue created on demand
- GIVEN a local queue provider
- WHEN a queue is requested for a transport name that has not been requested before
- THEN a new bounded queue SHALL be created and returned
- AND a subsequent request for the same name SHALL return the same queue instance

### Requirement: Local Queue Incoming Transport
The incoming local queue transport SHALL block waiting for events on the shared queue and SHALL deliver them to the consumer. The transport SHALL run its consumer loop on an internal executor. When constructed with only a queue, a virtual-thread-per-task executor SHALL be used; when constructed via the transport builder without a custom executor, a single-thread executor SHALL be used.

#### Scenario: Deliver queued event
- GIVEN an incoming local queue transport started with a consumer
- WHEN an event is placed on the shared queue
- THEN the transport SHALL deliver the event to the consumer

#### Scenario: Start is idempotent
- GIVEN a started incoming local queue transport
- WHEN start is invoked again
- THEN the transport SHALL NOT start a second consumer loop

### Requirement: Local Queue Outgoing Transport
The outgoing local queue transport SHALL place events on the shared bounded queue. If the queue is full, the send SHALL fail with a result indicating the queue rejected the event.

#### Scenario: Enqueue success
- GIVEN an outgoing local queue transport with capacity available
- WHEN an event is sent
- THEN the event SHALL be placed on the queue
- AND the result SHALL indicate success

#### Scenario: Queue full rejected
- GIVEN an outgoing local queue transport whose queue is full
- WHEN an event is sent
- THEN the result SHALL indicate failure
- AND the failure SHALL state that the queue is full and the event was rejected

### Requirement: Kafka Incoming Transport
The system SHALL provide an incoming Kafka transport that polls a topic and delivers events to the consumer. The transport SHALL support a topic list, a consumer group, and configurable Kafka consumer properties. Polling SHALL use a short timeout to remain responsive to stop requests.

#### Scenario: Kafka delivery
- GIVEN an incoming Kafka transport configured with a topic and consumer group
- WHEN the transport is started
- THEN it SHALL poll the topic and deliver deserialized events to the consumer

#### Scenario: Kafka stop
- GIVEN a started incoming Kafka transport
- WHEN the transport is stopped
- THEN polling SHALL cease and the consumer SHALL be released

### Requirement: Kafka Outgoing Transport
The system SHALL provide an outgoing Kafka transport that sends events to a topic using an asynchronous producer callback API. Sending on a closed transport SHALL fail.

#### Scenario: Kafka send success
- GIVEN an outgoing Kafka transport configured with a topic
- WHEN an event is sent
- THEN the event SHALL be produced to the topic
- AND the result SHALL indicate success with partition, offset, and topic metadata

#### Scenario: Send on closed transport
- GIVEN a closed outgoing Kafka transport
- WHEN an event is sent
- THEN an `IllegalStateException` SHALL be thrown

#### Scenario: Kafka producer failure
- GIVEN an outgoing Kafka transport whose producer rejects the send
- WHEN an event is sent
- THEN the result SHALL indicate failure with error details

### Requirement: Kafka Broadcast Outgoing Transport
The system SHALL provide an outgoing Kafka transport that broadcasts an event to every partition of the topic. Sending with no available partitions SHALL fail.

#### Scenario: Broadcast to all partitions
- GIVEN a broadcast Kafka transport with multiple partitions on the topic
- WHEN an event is sent
- THEN the event SHALL be produced to every partition

#### Scenario: No partitions
- GIVEN a broadcast Kafka transport with a topic that has no partitions
- WHEN an event is sent
- THEN a transport exception SHALL be thrown

#### Scenario: Partial partition failure
- GIVEN a broadcast Kafka transport where some partitions succeed and some fail
- WHEN an event is sent
- THEN a warning SHALL be logged
- AND a successful send result SHALL be returned

#### Scenario: Total partition failure
- GIVEN a broadcast Kafka transport where all partitions fail
- WHEN an event is sent
- THEN the result SHALL indicate failure with the underlying cause

### Requirement: Transport Builder
The system SHALL provide a builder that constructs a matching pair of incoming and outgoing local queue transports sharing one queue. The default queue capacity SHALL be 1,000.

#### Scenario: Shared queue pair
- GIVEN a local queue transports builder
- WHEN the pair is built
- THEN the incoming and outgoing transports SHALL share the same queue instance

#### Scenario: Custom capacity
- GIVEN a local queue transports builder configured with a queue capacity
- WHEN the pair is built without a custom queue
- THEN the shared queue SHALL be bounded at the configured capacity

#### Scenario: Custom executor
- GIVEN a local queue transports builder configured with an executor service
- WHEN the pair is built
- THEN the incoming transport SHALL use the configured executor service
