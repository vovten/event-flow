# Lifecycle

## Purpose

Define how the framework tracks events end-to-end through their lifecycle: persistence of events across the publishing and handling phases, status transitions (`NEW → PUBLISHED → HANDLED / FAILED`), acknowledgment-driven completion, automatic and manual retry, cleanup of terminal events, and service-scoped isolation when multiple services share the same store.

## Requirements

### Requirement: Lifecycle level selection

Each event carries a lifecycle level that determines how much tracking is performed. The level is resolved deterministically from the event itself.

#### Scenario: Selecting the level from an annotation

- **WHEN** an event class (or an envelope's payload class) is annotated with `@Event` and sets a lifecycle level
- **THEN** that annotation value is used as the event's lifecycle level

#### Scenario: Selecting the level from the event interface

- **WHEN** an event has no `@Event` annotation but overrides `Event.lifecycle()`
- **THEN** the overridden value is used as the event's lifecycle level

#### Scenario: Falling back for plain payloads

- **WHEN** an envelope wraps a plain Java object that has neither an annotation nor a lifecycle override
- **THEN** the lifecycle level defaults to `PERSISTED`

### Requirement: Persistence by lifecycle level

Each lifecycle level defines a distinct persistence contract: no persistence, persistence for reference only, or persistence with full tracking.

#### Scenario: Publishing a NONE-level event

- **WHEN** an event with lifecycle `NONE` is published through a lifecycle-aware publisher
- **THEN** the event is not persisted and no lifecycle tracking is performed

#### Scenario: Publishing a PERSISTED-level event

- **WHEN** an event with lifecycle `PERSISTED` is published through a lifecycle-aware publisher
- **THEN** the event is stored with status `UNDEFINED`
- **THEN** no further status transitions are performed for the event

#### Scenario: Publishing a MANAGED-level event

- **WHEN** an event with lifecycle `MANAGED` is published through a lifecycle-aware publisher
- **THEN** the event is stored with status `NEW`
- **THEN** the publication outcome is tracked and recorded

#### Scenario: Skipping persistence for lifecycle ack events

- **WHEN** a lifecycle acknowledgment event is published through a lifecycle-aware publisher
- **THEN** the ack event is passed through without being persisted

### Requirement: Publication status tracking

For `MANAGED` events, the publisher records the outcome of the delivery attempt in the event store.

#### Scenario: Recording a successful delivery

- **WHEN** a `MANAGED` event is successfully delivered through all target channels
- **THEN** the stored event's status transitions from `NEW` to `PUBLISHED`

#### Scenario: Recording a failed delivery

- **WHEN** delivery of a `MANAGED` event fails (all channels report failure or an error is thrown)
- **THEN** the stored event's status transitions from `NEW` to `FAILED`
- **THEN** the failure details are recorded on the stored event

### Requirement: Acknowledgment-driven completion tracking

Once a `MANAGED` event has been published, the handling side reports completion back through lifecycle ack events so the publisher-side store can be updated.

#### Scenario: Reporting successful handling

- **WHEN** a `MANAGED` traceable event has been successfully handled by all registered handlers
- **THEN** a `SuccessAck` referencing the original event is published back through the source channels
- **THEN** the publisher-side handler updates the stored event's status to `HANDLED`

#### Scenario: Reporting failed handling

- **WHEN** handling a `MANAGED` traceable event fails (an error propagates or a handler reports failure)
- **THEN** a `FailureAck` referencing the original event and carrying the failure details is published back through the source channels
- **THEN** the publisher-side handler updates the stored event's status to `FAILED` and records the error details

#### Scenario: Avoiding ack loops

- **WHEN** a lifecycle ack event is dispatched by the event dispatcher
- **THEN** the dispatcher does not wrap the ack event to generate another ack, preventing infinite acknowledgment loops

### Requirement: Automatic retry of failed and incomplete events

Events that failed to publish or that are stuck in an intermediate state are retried automatically according to a retry budget and an exponential backoff schedule.

#### Scenario: Retrying a failed event

- **WHEN** the retry scheduler runs and finds an event in `FAILED`, `PUBLISHED`, or `NEW` status that belongs to the local service and whose backoff period has elapsed
- **THEN** the event is deserialized from its stored payload and re-published through the configured publisher
- **THEN** the stored event's status is reset to `NEW` and its retry count is incremented

#### Scenario: Applying exponential backoff

- **WHEN** a stuck event is not yet ready for retry
- **THEN** the retry scheduler skips it until the backoff period computed as `minAge × 2^retryCount` has elapsed

#### Scenario: Exhausting the retry budget

- **WHEN** an event's retry count has reached the configured maximum
- **THEN** the event is no longer retried automatically and remains in `FAILED` for manual inspection

#### Scenario: Restoring channel routing on retry

- **WHEN** a retried event had explicitly declared channels
- **THEN** the deserialized event is re-published with those original channels restored

### Requirement: Manual retry

Operators can force a retry of a specific event regardless of its current status or retry budget, without consuming the automatic retry budget.

#### Scenario: Marking an event for manual retry

- **WHEN** an application calls `markForRetry(eventId)` on the event store
- **THEN** the stored event is flagged for retry, its error details are cleared, and its status and retry count are left unchanged

#### Scenario: Retrying a manually marked event

- **WHEN** the retry scheduler finds an event with the manual retry flag set
- **THEN** the event is re-published regardless of its current status, backoff state, or remaining retry budget
- **THEN** the retry count is not incremented for a manual retry

### Requirement: Cleanup of terminal events

The store is cleaned automatically to prevent unbounded growth, removing only events that can never transition further.

#### Scenario: Deleting old terminal events

- **WHEN** the cleanup scheduler runs and finds events in `HANDLED` or `UNDEFINED` status older than the configured maximum age
- **THEN** those events are deleted from the store in batches
- **THEN** a single cleanup cycle deletes at most one configured batch size, and any remaining events are picked up by later cycles

#### Scenario: Preserving failed events

- **WHEN** the cleanup scheduler scans the store
- **THEN** events in `FAILED` status are never deleted automatically

### Requirement: Service-scoped lifecycle tracking

When several services share the same event store, lifecycle operations must only affect events that belong to the local service, so that one service cannot mutate or retry another service's events.

#### Scenario: Stamping events with the originating service

- **WHEN** lifecycle-aware publishing is enabled with a service name
- **THEN** each published envelope is enriched with metadata identifying the originating service
- **THEN** that identity is carried inside acknowledgment events so handlers can attribute them

#### Scenario: Ignoring acknowledgments from other services

- **WHEN** an ack event arrives whose original service differs from the locally configured service name
- **THEN** the ack is ignored and the local store is not modified

#### Scenario: Retrying only local events

- **WHEN** the retry scheduler scans a store shared with other services
- **THEN** only events originating from the locally configured service name are selected for retry