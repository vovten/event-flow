# Lifecycle Tracking Specification

## Purpose

Lifecycle tracking provides end-to-end visibility into event processing: events are persisted to a store, their status advances through defined states, failed events are retried with exponential backoff, terminal events are cleaned up, and acknowledgements from the dispatcher update the stored status. This spec defines lifecycle levels, the event store contract, status transitions, acknowledgement handling, retry scheduling, and cleanup scheduling.

## Requirements

### Requirement: Lifecycle Levels
The system SHALL define three lifecycle levels for events: `NONE` (fire-and-forget, not persisted), `PERSISTED` (persisted without tracking), and `MANAGED` (fully tracked with acknowledgements).

#### Scenario: None lifecycle not persisted
- GIVEN an event with lifecycle `NONE`
- WHEN it passes through a lifecycle-aware publisher
- THEN it SHALL NOT be written to the event store

#### Scenario: Persisted lifecycle stores undefined
- GIVEN an event with lifecycle `PERSISTED`
- WHEN it passes through a lifecycle-aware publisher
- THEN it SHALL be stored with status `UNDEFINED`

#### Scenario: Managed lifecycle stores new
- GIVEN an event with lifecycle `MANAGED`
- WHEN it passes through a lifecycle-aware publisher
- THEN it SHALL be stored with status `NEW`

### Requirement: Event Statuses
The system SHALL define the event statuses `UNDEFINED`, `NEW`, `PUBLISHED`, `HANDLED`, and `FAILED`, each with a single-character code. Conversion from an unknown code SHALL fail.

#### Scenario: Status codes
- GIVEN the status enum
- WHEN each status's code is inspected
- THEN `UNDEFINED` SHALL be `U`, `NEW` SHALL be `N`, `PUBLISHED` SHALL be `P`, `HANDLED` SHALL be `H`, and `FAILED` SHALL be `F`

#### Scenario: Unknown code rejected
- GIVEN a character that is not a defined status code
- WHEN it is converted to a status
- THEN an `IllegalArgumentException` SHALL be thrown

### Requirement: Managed Status Flow
For managed events, the system SHALL advance statuses along the flow `NEW → PUBLISHED → HANDLED`. Failures SHALL transition an event to `FAILED`, and a failed event SHALL be eligible for retry back to `NEW`.

#### Scenario: Successful publication
- GIVEN a managed event stored as `NEW`
- WHEN the origin publication succeeds
- THEN the stored status SHALL become `PUBLISHED`

#### Scenario: Successful handling
- GIVEN a managed event stored as `PUBLISHED`
- WHEN the dispatcher handles it successfully and publishes a success acknowledgement
- THEN the stored status SHALL become `HANDLED`

#### Scenario: Failed handling
- GIVEN a managed event stored as `PUBLISHED`
- WHEN the dispatcher fails to handle it and publishes a failure acknowledgement
- THEN the stored status SHALL become `FAILED`
- AND the failure details SHALL be recorded

### Requirement: Event Store Contract
The event store SHALL persist stored events and support status updates, lookups by id, lookups by status, retry marking, retryable lookup, and batched deletion. Saving a duplicate event id SHALL fail; updating an unknown event SHALL fail.

#### Scenario: Save and find by id
- GIVEN a stored event
- WHEN it is saved and then looked up by its event id
- THEN the stored event SHALL be returned

#### Scenario: Duplicate save rejected
- GIVEN an event id already saved
- WHEN a second event with the same id is saved
- THEN an `IllegalArgumentException` SHALL be thrown

#### Scenario: Update unknown event
- GIVEN an event id that does not exist
- WHEN its status is updated
- THEN a `NoSuchElementException` SHALL be thrown

#### Scenario: Query by status
- GIVEN several stored events with different statuses
- WHEN events are queried by a status and a before-timestamp
- THEN only events with that status older than the timestamp SHALL be returned

### Requirement: Retry Counting
When an event's status is updated to `NEW`, the store SHALL increment its retry count unless the event carries a manual retry flag. Manual retries SHALL NOT consume the automatic retry budget.

#### Scenario: Automatic retry increments count
- GIVEN an event without a manual retry flag
- WHEN its status is updated to `NEW`
- THEN its retry count SHALL be incremented

#### Scenario: Manual retry preserves count
- GIVEN an event marked for manual retry
- WHEN its status is updated to `NEW`
- THEN its retry count SHALL NOT be incremented

### Requirement: Manual Retry Marking
The store SHALL support marking an event for manual retry. Marking SHALL set the retry flag, clear error details, and leave the status and retry count unchanged. Marking an unknown event SHALL fail.

#### Scenario: Mark for retry
- GIVEN a stored failed event
- WHEN it is marked for retry
- THEN the retry flag SHALL be set
- AND error details SHALL be cleared
- AND the status SHALL remain unchanged

#### Scenario: Mark unknown event
- GIVEN an event id that does not exist
- WHEN it is marked for retry
- THEN a `NoSuchElementException` SHALL be thrown

### Requirement: Retry Scheduler
The system SHALL provide a scheduler that periodically rescues retryable events: events in failed, published, or new status, and events flagged for manual retry. The scheduler SHALL deserialize stored payloads and republish them through the event publisher.

#### Scenario: Scheduled republish
- GIVEN a retry scheduler configured with an interval and an event publisher
- WHEN a stored event becomes retryable
- THEN the scheduler SHALL republish the event on its next cycle

#### Scenario: Exponential retry backoff
- GIVEN a retry scheduler with a minimum age
- WHEN an event has failed repeatedly
- THEN the delay before each subsequent retry SHALL grow as the minimum age multiplied by a power of two based on the retry count

#### Scenario: Retry budget exhausted
- GIVEN a stored event whose automatic retry count has reached the configured maximum
- WHEN the scheduler evaluates the event
- THEN the event SHALL be skipped
- AND no further automatic retries SHALL be attempted

#### Scenario: Manual retry ignores limits
- GIVEN an event flagged for manual retry
- WHEN the scheduler evaluates the event
- THEN it SHALL be retried regardless of the automatic retry count and backoff schedule
- AND its retry count SHALL NOT be incremented

#### Scenario: Service required
- GIVEN a retry scheduler constructed without a service name
- WHEN it is constructed
- THEN an `IllegalArgumentException` SHALL be thrown

#### Scenario: Channel restoration on retry
- GIVEN a retry scheduler republishing an event whose stored payload carries channel declarations
- WHEN the event is republished
- THEN the declared channels SHALL be restored on the republished envelope
- AND unknown channel classes SHALL be skipped

### Requirement: Cleanup Scheduler
The system SHALL provide a scheduler that periodically deletes terminal events from the store. Only `HANDLED` and `UNDEFINED` events older than a maximum age SHALL be deleted. `FAILED` events SHALL never be deleted automatically.

#### Scenario: Delete handled events
- GIVEN a cleanup scheduler and a stored handled event older than the maximum age
- WHEN the scheduler runs
- THEN the event SHALL be deleted from the store

#### Scenario: Failed events retained
- GIVEN a cleanup scheduler and a stored failed event older than the maximum age
- WHEN the scheduler runs
- THEN the event SHALL NOT be deleted

#### Scenario: Recent events retained
- GIVEN a cleanup scheduler and a handled event younger than the maximum age
- WHEN the scheduler runs
- THEN the event SHALL NOT be deleted

#### Scenario: Bounded deletion per cycle
- GIVEN a cleanup scheduler
- WHEN the number of eligible events exceeds the per-cycle batch limit
- THEN at most the batch limit SHALL be deleted in one cycle

### Requirement: Success Acknowledgement
The system SHALL provide a success acknowledgement event that records the original event id, event type, originating service, source channels, and process id. The acknowledgement SHALL route through the source channels.

#### Scenario: Success ack fields
- GIVEN a success acknowledgement for an original event
- WHEN the acknowledgement is inspected
- THEN it SHALL carry the original event id and event type
- AND it SHALL route through the original event's source channels

### Requirement: Failure Acknowledgement
The system SHALL provide a failure acknowledgement event that records the original event id, event type, originating service, error details, and source channels.

#### Scenario: Failure ack fields
- GIVEN a failure acknowledgement for an original event
- WHEN the acknowledgement is inspected
- THEN it SHALL carry the original event id, event type, and error details

### Requirement: Dispatcher Acknowledgement Publishing
The system SHALL provide a dispatcher decorator that publishes a success or failure acknowledgement after dispatch completes, based on whether all handlers succeeded. Acknowledgements SHALL be published only for managed traceable events. Lifecycle acknowledgement events themselves SHALL NOT be wrapped again.

#### Scenario: Success ack on full success
- GIVEN a managed traceable event
- WHEN dispatch completes with all handlers successful
- THEN a success acknowledgement SHALL be published

#### Scenario: Failure ack on partial or total failure
- GIVEN a managed traceable event
- WHEN dispatch completes with at least one handler failing or an error
- THEN a failure acknowledgement SHALL be published
- AND it SHALL carry the failure chain details

#### Scenario: No ack for non-managed events
- GIVEN a traceable event with lifecycle `PERSISTED`
- WHEN dispatch completes
- THEN no acknowledgement SHALL be published

#### Scenario: Ack loop protection
- GIVEN an acknowledgement event being dispatched
- WHEN the decorator processes it
- THEN it SHALL pass through without publishing a further acknowledgement

### Requirement: Acknowledgement Handler
The system SHALL provide an acknowledgement handler that updates the stored event status when an acknowledgement arrives. A success acknowledgement SHALL set status `HANDLED`; a failure acknowledgement SHALL set status `FAILED` with error details. The handler SHALL filter acknowledgements by service name.

#### Scenario: Success ack handling
- GIVEN an acknowledgement handler and a success acknowledgement
- WHEN the acknowledgement is handled
- THEN the original event's stored status SHALL become `HANDLED`

#### Scenario: Failure ack handling
- GIVEN an acknowledgement handler and a failure acknowledgement
- WHEN the acknowledgement is handled
- THEN the original event's stored status SHALL become `FAILED`
- AND the error details SHALL be recorded

#### Scenario: Service filter
- GIVEN an acknowledgement handler configured with a service name
- WHEN an acknowledgement from a different service arrives
- THEN the acknowledgement SHALL be ignored

#### Scenario: Empty service accepts all
- GIVEN an acknowledgement handler configured without a service name
- WHEN an acknowledgement from any service arrives
- THEN it SHALL be processed

### Requirement: In-Memory Event Store
The system SHALL provide an in-memory event store implementation for tests and single-JVM deployments. Its type identifier SHALL be `"in-memory"`.

#### Scenario: In-memory type
- GIVEN an in-memory event store
- WHEN its type is accessed
- THEN the value `"in-memory"` SHALL be returned

### Requirement: JDBC Event Store
The system SHALL provide a JDBC-backed event store for relational databases. It SHALL support an explicit table name, optional schema auto-initialization, UUID storage strategies, and database-specific SQL dialects. Its type identifier SHALL be `"db"`.

#### Scenario: JDBC type
- GIVEN a JDBC event store
- WHEN its type is accessed
- THEN the value `"db"` SHALL be returned

#### Scenario: Schema auto-initialization
- GIVEN a JDBC event store with auto-initialization enabled and a schema that does not exist
- WHEN the store is constructed
- THEN the required table and indexes SHALL be created

#### Scenario: Schema verification
- GIVEN a JDBC event store with auto-initialization disabled
- WHEN the table does not exist
- THEN an illegal state exception SHALL be thrown during verification

#### Scenario: Dialect detection
- GIVEN a JDBC event store connected to a supported database
- WHEN the dialect is detected
- THEN the matching dialect SHALL be selected based on the database product name

### Requirement: Stored Event Immutability
A stored event SHALL be an immutable record of the event's persistence state: event id, event type, service, JSON payload, channels, process id, status, retry count, retry flag, timestamps, and error details. Transition helpers SHALL return new instances rather than mutating.

#### Scenario: Status transition creates copy
- GIVEN a stored event
- WHEN a new status is applied
- THEN a new stored event SHALL be returned with the new status and updated timestamp
- AND the original instance SHALL remain unchanged

#### Scenario: Retry transition
- GIVEN a stored event
- WHEN the retry transition is applied
- THEN a new instance SHALL be returned with status `NEW`, an incremented retry count, and an updated timestamp

#### Scenario: Retry readiness
- GIVEN a stored event with a retry count and a minimum age
- WHEN readiness is evaluated before the computed backoff window has elapsed
- THEN the event SHALL NOT be ready for retry
- AND after the window has elapsed it SHALL be ready
