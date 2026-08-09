# Dispatch Specification

## Purpose

Dispatching delivers events received from incoming transports to registered event handlers. This spec defines the `EventDispatcher` contract, the unified dispatcher that consumes multiple transports concurrently, idempotent deduplication, dispatch logging, and the composite dispatcher builder.

## Requirements

### Requirement: Dispatcher Contract
A dispatcher SHALL asynchronously deliver an event to all matching handlers and SHALL return a future of handler results. A dispatcher SHALL support registering listeners, checking whether a listener is registered, starting with a dispatch consumer, and stopping cleanly.

#### Scenario: Dispatch returns future
- GIVEN a dispatcher with at least one handler for the event type
- WHEN the event is dispatched
- THEN a `CompletableFuture<HandlerResults>` SHALL be returned
- AND the results SHALL contain one result per matching handler

#### Scenario: No matching handlers
- GIVEN a dispatcher with no handlers for the event type
- WHEN the event is dispatched
- THEN the future SHALL complete with empty handler results
- AND the results SHALL carry a skip reason of `no handlers found`

### Requirement: Concurrent Handler Execution
The unified dispatcher SHALL execute matching handlers concurrently on an externally supplied executor service. The dispatcher SHALL NOT own or close the executor service.

#### Scenario: Concurrent invocation
- GIVEN a dispatcher with multiple handlers for the event type
- WHEN the event is dispatched
- THEN each handler SHALL be invoked through the executor service
- AND the aggregated results SHALL be produced in handler registration order

#### Scenario: Executor rejection handled
- GIVEN a dispatcher whose executor rejects a handler submission
- WHEN the event is dispatched
- THEN the dispatch SHALL NOT throw
- AND the rejected handler SHALL be recorded as a failure in the results

### Requirement: Handler Failure Isolation
A dispatcher SHALL isolate handler failures. An exception thrown by one handler SHALL NOT prevent other handlers from executing, SHALL NOT roll back already produced results, and SHALL be captured as a failure result for that handler.

#### Scenario: One handler fails
- GIVEN a dispatcher with two handlers where the first throws
- WHEN the event is dispatched
- THEN the second handler SHALL still be invoked
- AND the results SHALL contain one success and one failure
- AND the failure SHALL reference the failing handler and the root cause

#### Scenario: All handlers fail
- GIVEN a dispatcher where every handler throws
- WHEN the event is dispatched
- THEN the results SHALL indicate an all-failure outcome
- AND the results SHALL NOT include any successes

### Requirement: Optional Concurrency Limit
The unified dispatcher SHALL support an optional concurrency limit that caps the number of concurrently executing handlers. With a null or absent limit, execution SHALL be unlimited.

#### Scenario: Concurrency capped
- GIVEN a dispatcher constructed with a concurrency semaphore
- WHEN many events are dispatched concurrently
- THEN the number of handlers executing simultaneously SHALL NOT exceed the configured limit

#### Scenario: Interrupted handler execution
- GIVEN a dispatcher with a concurrency limit
- WHEN a thread waiting for a slot is interrupted
- THEN the interruption flag SHALL be restored
- AND the handler SHALL be recorded as a failure

### Requirement: Dispatcher Lifecycle
The unified dispatcher SHALL start incoming transports idempotently. Starting twice SHALL NOT start the transports a second time. Stopping SHALL stop all transports and SHALL tolerate individual transport stop failures.

#### Scenario: Start is idempotent
- GIVEN a started dispatcher
- WHEN start is invoked again
- THEN the transports SHALL NOT be started again
- AND a warning SHALL be logged

#### Scenario: Stop tolerates transport errors
- GIVEN a dispatcher whose one transport fails to stop
- WHEN the dispatcher is stopped
- THEN the remaining transports SHALL still be stopped
- AND the failure SHALL be logged rather than propagated

### Requirement: Idempotent Deduplication
The system SHALL provide an idempotent dispatcher decorator that prevents duplicate delivery of already processed events. Deduplication SHALL key on the traceable event identifier and SHALL be bounded by a time-to-live and a maximum cache size. Events without a traceable identifier SHALL NOT be deduplicated.

#### Scenario: Duplicate event skipped
- GIVEN an idempotent dispatcher that already processed an event with a given id
- WHEN the same event id is dispatched again within the TTL
- THEN the event SHALL NOT be delivered to handlers
- AND the results SHALL carry a skip reason of `duplicate event`
- AND a warning SHALL be logged when configured to do so

#### Scenario: Key released on total failure
- GIVEN an idempotent dispatcher
- WHEN an event's dispatch results in all handlers failing
- THEN the deduplication key SHALL be released
- AND a subsequent dispatch of the same event id SHALL be delivered again

#### Scenario: Key released on infrastructure failure
- GIVEN an idempotent dispatcher
- WHEN an event's dispatch fails with an infrastructure error
- THEN the deduplication key SHALL be released

#### Scenario: Key retained on success
- GIVEN an idempotent dispatcher
- WHEN an event's dispatch succeeds fully
- THEN the deduplication key SHALL be retained
- AND a subsequent duplicate dispatch SHALL be skipped

#### Scenario: Non-traceable events pass through
- GIVEN an idempotent dispatcher and an event that does not expose a traceable identifier
- WHEN the event is dispatched repeatedly
- THEN each dispatch SHALL be delivered to handlers

### Requirement: Dispatch Logging
The system SHALL provide a logging dispatcher decorator that emits a machine-parseable JSON log record after dispatch completes. Records SHALL contain status, event identifier, payload (truncated), handler outcomes, failed handler names with root causes, failure counts, and duration.

#### Scenario: Success log
- GIVEN a logging dispatcher around an origin where all handlers succeed
- WHEN an event is dispatched
- THEN an INFO log record SHALL be emitted
- AND the status SHALL be `handled`

#### Scenario: Partial failure log
- GIVEN a logging dispatcher where some handlers fail
- WHEN an event is dispatched
- THEN a WARN log record SHALL be emitted
- AND the status SHALL be `partial`
- AND the record SHALL list the failed handlers with root causes

#### Scenario: Failure log
- GIVEN a logging dispatcher where the dispatch fails
- WHEN an event is dispatched
- THEN an ERROR log record SHALL be emitted
- AND the status SHALL be `failed`

#### Scenario: Excluded event types
- GIVEN a logging dispatcher configured with excluded event types
- WHEN an event of an excluded type is dispatched
- THEN the dispatch SHALL be delegated without emitting a log record

### Requirement: Dispatcher Builder
The system SHALL provide a fluent dispatcher builder that composes the base dispatcher with idempotent and logging decorators. The builder SHALL require an executor service and a handler registry.

#### Scenario: Builder requires executor
- GIVEN a dispatcher builder without an executor service
- WHEN the dispatcher is built
- THEN an `IllegalStateException` SHALL be thrown

#### Scenario: Builder requires registry
- GIVEN a dispatcher builder without a handler registry
- WHEN the dispatcher is built
- THEN an `IllegalStateException` SHALL be thrown

#### Scenario: Compose decorators
- GIVEN a builder with idempotent and logging decorators enabled
- WHEN the dispatcher is built
- THEN the idempotent decorator SHALL wrap the base dispatcher
- AND the logging decorator SHALL wrap the outermost layer

#### Scenario: Default idempotent configuration
- GIVEN a builder with idempotency enabled without explicit parameters
- WHEN the dispatcher is built
- THEN the deduplication TTL SHALL be 10 minutes
- AND the cache size SHALL be 10,000 entries
- AND duplicate warnings SHALL be enabled

### Requirement: Handler Results Inspection
Handler results SHALL support aggregated inspection: emptiness, all-success, all-failure, partial-success, success and failure lists, first success/failure, first error, and a human-readable summary.

#### Scenario: Outcome classification
- GIVEN handler results containing both successes and failures
- WHEN the outcome is inspected
- THEN `isPartialSuccess()` SHALL return true
- AND `isAllSuccess()` SHALL return false
- AND `isAllFailure()` SHALL return false

#### Scenario: Empty results
- GIVEN handler results created from a null result list
- WHEN emptiness is inspected
- THEN the results SHALL be empty
- AND `isAllSuccess()` SHALL return false
