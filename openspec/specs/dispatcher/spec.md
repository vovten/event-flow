# Dispatcher

## Purpose

Define how events received from incoming transports are delivered to registered handlers, including asynchronous execution, concurrency backpressure, idempotent processing, structured logging of dispatched events, and decorator-driven composition of the dispatch chain.

## Requirements

### Requirement: Asynchronous event dispatch

The dispatcher receives an event and delivers it to the appropriate handlers without blocking the caller.

#### Scenario: Dispatching an event to handlers

- **WHEN** an application dispatches an event through the dispatcher
- **THEN** the call returns a `CompletableFuture<HandlerResults>` without blocking the caller
- **THEN** the future completes with the aggregated handler results once every applicable handler has finished

#### Scenario: Finding no handlers

- **WHEN** an event is dispatched but no registered handler handles its type
- **THEN** the returned future completes with empty handler results indicating that no handlers were found

### Requirement: Enabling transports

The dispatcher starts and stops the incoming transports it is configured with.

#### Scenario: Starting the dispatcher

- **WHEN** an application calls `start(dispatchConsumer)` on the dispatcher
- **THEN** all configured incoming transports begin receiving events
- **THEN** received events are delivered to the dispatch consumer for processing

#### Scenario: Stopping the dispatcher

- **WHEN** an application calls `stop()` on the dispatcher
- **THEN** all configured incoming transports are gracefully shut down and their resources released

### Requirement: Async handler execution

Handler execution runs on a provided executor, allowing handlers to run concurrently without coupling to the transport thread.

#### Scenario: Running handlers on a provided executor

- **WHEN** an event is dispatched and the dispatcher has an executor configured
- **THEN** handler execution is submitted to that executor

#### Scenario: Building a dispatcher without an executor

- **WHEN** an application calls `build()` on a dispatcher builder that has no executor configured
- **THEN** an `IllegalStateException` is thrown and no dispatcher is created

### Requirement: Concurrency backpressure

A configurable concurrency limit caps the number of concurrent handler executions, providing backpressure so a burst of events cannot overwhelm downstream systems.

#### Scenario: Limiting concurrent handler executions

- **WHEN** a dispatcher is built with a concurrency limit and the limit is reached by in-flight handler executions
- **THEN** further handler submissions wait until a slot is released

#### Scenario: Building a dispatcher with a non-positive limit

- **WHEN** an application configures a concurrency limit that is not positive
- **THEN** an `IllegalArgumentException` is thrown

### Requirement: Idempotent event processing

The dispatcher can deduplicate events that have already been processed, preventing duplicate side effects when the same event is received more than once.

#### Scenario: Processing a duplicate event

- **WHEN** a dispatcher with idempotency enabled receives a traceable event whose identifier was already processed within the cache window
- **THEN** the duplicate is not dispatched again
- **THEN** the returned future completes with handler results marked as a duplicate

#### Scenario: Processing an event for the first time

- **WHEN** a dispatcher with idempotency enabled receives a traceable event whose identifier was not seen before
- **THEN** the event is dispatched normally
- **THEN** the identifier is recorded so later repeats are recognized as duplicates

#### Scenario: Retrying after a failed dispatch

- **WHEN** a previously seen event is dispatched again after its earlier dispatch failed
- **THEN** the event is dispatched again, since the earlier failure is not treated as a completed duplicate

### Requirement: Dispatcher composition

Dispatchers are assembled from a base unified dispatcher plus optional decorators for idempotency, logging, and custom behaviour, controlled either through a fixed decorator order or an explicit chain.

#### Scenario: Building a dispatcher without a registry

- **WHEN** an application calls `build()` on a dispatcher builder that has no handler registry configured
- **THEN** an `IllegalStateException` is thrown and no dispatcher is created

#### Scenario: Applying decorators in a fixed order

- **WHEN** an application enables idempotency and logging on the standard dispatcher builder
- **THEN** the base dispatcher is wrapped first by the idempotent decorator and then by the logging decorator

#### Scenario: Controlling decorator order explicitly

- **WHEN** an application builds a dispatcher using the explicit chain mode
- **THEN** decorators are applied in exactly the order they were added, wrapping the base dispatcher innermost

### Requirement: Structured logging of dispatched events

The dispatcher can log every handled event as structured output to aid observability without leaking full payloads.

#### Scenario: Logging a dispatched event

- **WHEN** logging is enabled and an event is dispatched
- **THEN** a structured log entry is emitted that includes the handler outcomes and delivery status
- **THEN** the payload is truncated to the configured maximum payload length

#### Scenario: Excluding event types from logging

- **WHEN** logging is enabled and a dispatched event type is listed as excluded
- **THEN** no log entry is emitted for that event

### Requirement: Aggregated handler results

The outcome of delivering an event to multiple handlers is represented as an aggregate that applications can inspect to determine overall handling success.

#### Scenario: Recording handler outcomes

- **WHEN** handlers have finished processing an event
- **THEN** the aggregated results report the number of successful and failed handler executions, and whether the outcome was all-success, partial, or all-failure