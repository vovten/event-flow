# Publishing

## Purpose

Define how applications publish events through the framework: asynchronous delivery to configured channels and transports, automatic envelope wrapping, fine-grained control over envelope metadata, channel-based routing, retry with exponential backoff, transactional publishing in Spring, and structured logging of published events.

## Requirements

### Requirement: Asynchronous event publishing API

The framework exposes a single non-blocking entry point for publishing events. Publishing never blocks the caller: every `publish` call returns immediately and delivers events asynchronously.

#### Scenario: Publishing an event

- **WHEN** an application calls `publisher.publish(event)`
- **THEN** the call returns a `CompletableFuture<SendResults>` without blocking the caller
- **THEN** the future completes with the aggregated send results once the event has been delivered to all target destinations

#### Scenario: Publishing a plain object

- **WHEN** an application calls `publisher.publish(payload)` with a plain Java object
- **THEN** the payload is wrapped in an `Envelope` with an auto-generated `eventId`, a null `processId`, and the current `occurredAt` timestamp
- **THEN** the envelope is delivered through the normal channel routing

#### Scenario: Publishing an object correlated to a process

- **WHEN** an application calls `publisher.publish(processId, payload)`
- **THEN** the payload is wrapped in an `Envelope` that carries the given `processId`, an auto-generated `eventId`, and the current `occurredAt` timestamp
- **THEN** the envelope is delivered through the normal channel routing

### Requirement: Fine-grained envelope control

Applications can construct an event with explicit envelope metadata before publishing instead of relying on auto-generated values.

#### Scenario: Building an event with custom metadata

- **WHEN** an application calls `publisher.prepare(payload)` and uses the returned builder to set a `processId` and additional metadata entries
- **THEN** calling `publish()` on the builder publishes an `Envelope` containing the configured metadata

#### Scenario: Building an event with explicit channels

- **WHEN** an application builds an envelope with an explicit channel list
- **THEN** the event is routed only to those channels, overriding the payload's default channel resolution

### Requirement: Channel-based routing

Events are routed through channels, each channel owning a set of outgoing transports. Channel routing is determined per event: either explicitly on the envelope or by the event type's default channel declaration.

#### Scenario: Routing to a configured channel

- **WHEN** an event declares a channel that is registered with the publisher
- **THEN** the event is delivered to that channel's transports
- **THEN** the aggregated `SendResults` includes the outcome of every transport send

#### Scenario: Routing to multiple channels

- **WHEN** an event declares multiple channels
- **THEN** the event is delivered to each declared channel
- **THEN** the returned future completes only after delivery to all declared channels has finished

#### Scenario: Publishing an event without channels

- **WHEN** an application invokes the publish API without specifying channels
- **THEN** the event is routed to the default internal channel

### Requirement: Fail-fast on missing channel configuration

A misconfiguration where an event declares a channel that is not registered must surface as an explicit error rather than silently dropping the event.

#### Scenario: Declaring an unconfigured channel

- **WHEN** an event declares a channel that is not registered with the publisher
- **THEN** the returned future completes exceptionally with an `EventPublisherConfigException`
- **THEN** the error message names both the missing channel and the affected event type

### Requirement: Retry with exponential backoff

Publishing failures caused by transient conditions can be retried automatically with an exponentially growing delay between attempts, bounded by an absolute maximum delay.

#### Scenario: Retrying a transient failure

- **WHEN** the underlying delivery fails with a retryable error (for example a network or timeout failure)
- **THEN** the publisher schedules a retry after a delay of `initialDelay × multiplier^(attempt - 1)`
- **THEN** the delay never exceeds the configured maximum delay

#### Scenario: Giving up after exhausting retries

- **WHEN** all configured retry attempts are exhausted and the delivery still fails
- **THEN** the returned future completes exceptionally with a publisher exception describing the number of attempts made

#### Scenario: Not retrying non-retryable errors

- **WHEN** the underlying delivery fails with a configuration error or an illegal argument
- **THEN** the publisher does not schedule any retry and the future completes exceptionally with the original error

### Requirement: Publisher composition

Publishers are assembled from a base channel publisher plus optional decorators for retry, lifecycle tracking, logging, and transaction handling. Configuration errors are detected at build time, not at publish time.

#### Scenario: Building a publisher without channels

- **WHEN** an application calls `build()` on a publisher builder that has no channels configured
- **THEN** an `IllegalStateException` is thrown and no publisher is created

#### Scenario: Combining retry and lifecycle tracking

- **WHEN** an application enables both retry and lifecycle-aware publishing on the builder
- **THEN** the built publisher persists and tracks events through the lifecycle layer and retries transient delivery failures underneath it

#### Scenario: Adding custom decorators

- **WHEN** an application registers custom decorators on the builder
- **THEN** each registered decorator is applied around the base publisher in the registration order

### Requirement: Transactional publishing

In the Spring integration, publishing can be coordinated with the surrounding database transaction so events are only sent after the transaction commits successfully.

#### Scenario: Publishing inside an active transaction

- **WHEN** a transactional publisher is used inside an active Spring transaction
- **THEN** delivery is deferred until after the transaction commits
- **THEN** the returned future completes with the send results after commit

#### Scenario: Rolling back the transaction

- **WHEN** the surrounding transaction rolls back or fails to commit
- **THEN** the event is never delivered
- **THEN** the returned future completes exceptionally, signalling that publication was aborted

#### Scenario: Publishing outside a transaction

- **WHEN** a transactional publisher is used and no Spring transaction is active
- **THEN** the event is delivered immediately

### Requirement: Structured logging of published events

Publishing can log every dispatched event as structured output to aid observability without leaking full payloads.

#### Scenario: Logging a published event

- **WHEN** logging is enabled and an event is published
- **THEN** a structured log entry is emitted that includes the event identity, target channels, and delivery status
- **THEN** the payload is truncated to the configured maximum payload length

#### Scenario: Excluding event types from logging

- **WHEN** logging is enabled and a published event type is listed as excluded
- **THEN** no log entry is emitted for that event