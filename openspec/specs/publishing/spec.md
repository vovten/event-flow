# Publishing Specification

## Purpose

Publishing is the mechanism by which events are handed to channels and then to outgoing transports. This spec defines the `EventPublisher` contract, channel-based routing, retry with exponential backoff, structured logging, and the composite publisher builder.

## Requirements

### Requirement: Publisher Contract
An event publisher SHALL publish events asynchronously and SHALL return a future of send results. The publisher SHALL also accept plain payloads by wrapping them in an envelope, and SHALL offer a way to prepare a fluent event builder bound to the publisher.

#### Scenario: Publish returns future
- GIVEN an event publisher
- WHEN an event is published
- THEN a `CompletableFuture<SendResults>` SHALL be returned

#### Scenario: Payload convenience overloads
- GIVEN an event publisher
- WHEN a plain payload is published without an envelope
- THEN an envelope SHALL be created with a random event id and current timestamp
- AND the envelope SHALL be routed per the payload's channel declaration

### Requirement: Channel Routing
The publisher SHALL route each event through every channel declared by the event. The publisher SHALL register channels by their concrete class, and a channel lookup SHALL be thread-safe.

#### Scenario: Route through declared channels
- GIVEN a publisher with an internal channel registered and an event declaring internal routing
- WHEN the event is published
- THEN the event SHALL be delivered to the internal channel
- AND the results SHALL aggregate the channel's send results

#### Scenario: Missing channel produces failed future
- GIVEN a publisher that has not registered the channel class declared by an event
- WHEN the event is published
- THEN the returned future SHALL complete exceptionally with a configuration exception
- AND the exception SHALL identify both the missing channel and the event type

### Requirement: Retry with Exponential Backoff
The system SHALL provide a retrying publisher decorator that retries failed publications with exponential backoff. The default configuration SHALL be three retries with an initial delay of 100 milliseconds and a multiplier of 2.0, capped at a maximum delay of 10 seconds.

#### Scenario: Retry on transient failure
- GIVEN a retrying publisher with default settings around a failing origin
- WHEN an event is published
- THEN the publication SHALL be retried up to the configured maximum attempts
- AND delays between attempts SHALL grow exponentially up to the cap

#### Scenario: Retry budget exhausted
- GIVEN a retrying publisher where the origin keeps failing
- WHEN the publication is attempted
- THEN after the configured attempts the future SHALL complete exceptionally
- AND the exception SHALL report the number of attempts made

#### Scenario: Non-retryable errors fail fast
- GIVEN a retrying publisher wrapping an origin that throws a configuration exception
- WHEN an event is published
- THEN the publication SHALL NOT be retried
- AND the future SHALL complete exceptionally with the original configuration exception

#### Scenario: Invalid retry configuration rejected
- GIVEN a retry configuration with a negative max retries, a non-positive initial delay, a multiplier below 1.0, or a non-positive maximum delay
- WHEN the retrying publisher is constructed
- THEN an `IllegalArgumentException` SHALL be thrown

### Requirement: Structured Publishing Logs
The system SHALL provide a logging publisher decorator that emits a machine-parseable JSON log record after a publication completes. Records SHALL contain status, event identifier, payload (truncated to a configurable maximum length), channel list, delivery destinations, failures, and error details.

#### Scenario: Success log
- GIVEN a logging publisher wrapping a successful origin
- WHEN an event is published
- THEN an INFO log record SHALL be emitted
- AND the status SHALL be `published`

#### Scenario: Partial success log
- GIVEN a logging publisher where some channel deliveries failed
- WHEN an event is published
- THEN a WARN log record SHALL be emitted
- AND the status SHALL be `partial`

#### Scenario: Failure log
- GIVEN a logging publisher where the publication fails
- WHEN an event is published
- THEN an ERROR log record SHALL be emitted
- AND the status SHALL be `failed`
- AND the record SHALL include the error message and type

#### Scenario: Payload truncation
- GIVEN a logging publisher configured with a maximum payload length
- WHEN an event with a longer payload is published
- THEN the logged payload SHALL be truncated to the configured length with an ellipsis suffix

#### Scenario: Excluded event types
- GIVEN a logging publisher configured with a set of excluded event types
- WHEN an event of an excluded type is published
- THEN the publication SHALL be delegated without emitting a log record

#### Scenario: Log level override
- GIVEN a logging publisher configured with per-event log levels
- WHEN an event whose type maps to a suppressing level fails
- THEN the failure SHALL be logged at the configured level
- AND log records for non-suppressing levels SHALL be emitted normally

### Requirement: Publisher Builder
The system SHALL provide a fluent publisher builder that composes channel routing, retry, lifecycle awareness, and custom decorators into a single publisher. Building without any registered channels SHALL fail.

#### Scenario: Compose decorated publisher
- GIVEN a builder with channels and a retry decorator enabled
- WHEN the publisher is built
- THEN the built publisher SHALL route through the channels
- AND the retry decorator SHALL wrap the routing publisher

#### Scenario: Build without channels rejected
- GIVEN a publisher builder with no channels
- WHEN the publisher is built
- THEN an `IllegalStateException` SHALL be thrown

#### Scenario: Custom decorator order
- GIVEN a builder with custom decorators added in sequence
- WHEN the publisher is built
- THEN the decorators SHALL be applied in the order they were added, with the innermost applied first

### Requirement: Lifecycle-Aware Publishing
The system SHALL provide a publisher decorator that persists events into an event store according to their lifecycle level before forwarding them to the origin publisher.

#### Scenario: Persisted events stored as undefined
- GIVEN a lifecycle-aware publisher and an event with lifecycle `PERSISTED`
- WHEN the event is published
- THEN the event SHALL be saved to the store with status `UNDEFINED`
- AND the event SHALL be forwarded to the origin publisher

#### Scenario: Managed events stored as new
- GIVEN a lifecycle-aware publisher and an event with lifecycle `MANAGED`
- WHEN the event is published
- THEN the event SHALL be saved to the store with status `NEW`
- AND the event SHALL be forwarded to the origin publisher

#### Scenario: Failure updates status
- GIVEN a lifecycle-aware publisher where the origin publication fails
- WHEN the event publication completes
- THEN the event's status in the store SHALL be updated to `FAILED` with error details

#### Scenario: Success updates status
- GIVEN a lifecycle-aware publisher where the origin publication succeeds
- WHEN the event publication completes
- THEN the event's status in the store SHALL be updated to `PUBLISHED`

#### Scenario: None lifecycle bypasses store
- GIVEN a lifecycle-aware publisher and an event with lifecycle `NONE`
- WHEN the event is published
- THEN the event SHALL NOT be persisted
- AND the event SHALL be forwarded to the origin publisher

#### Scenario: Service metadata added
- GIVEN a lifecycle-aware publisher configured with a service name and an envelope payload
- WHEN the event is published
- THEN the envelope SHALL carry a metadata entry identifying the publishing service
