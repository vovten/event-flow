# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] (target 1.4.0)

### Added

- Kafka partition keys via `EventKeyResolver`: the record key can now be resolved from the event's `processId`, event type, `Envelope` metadata, or a custom resolver. This lets all events of the same process/saga or type land in the same Kafka partition (ordering guarantee). The default `none` strategy preserves the previous round-robin behavior. Spring auto-configuration exposes the `key` transport option (`none`, `process-id`, `event-type`, `metadata:<name>`, `bean:<beanName>`).
- `AcceptedAck`: a consumer-side delivery confirmation published before handler execution. Together with `SuccessAck` / `FailureAck` it separates "was the event delivered?" from "was the event processed?".
- `NoHandlerAck`: published when a managed event is received but no listener is resolved. The event moves to the terminal `DEAD` status instead of being treated as successfully handled.
- `EventStatus.ACCEPTED` and `EventStatus.DEAD`: explicit delivery-confirmed and terminal dead-letter states. No schema migration is required; only status comments change.
- `EventStore.transitionStatus(...)`: conditional status updates that reject stale or out-of-order acknowledgments instead of overwriting a newer state.
- Named `@RetryPolicy` processing policies. The `name` attribute is required; a method-level declaration declares and binds the policy, a type-level declaration makes it reusable through `@EventListener(retryPolicy = "...")`. `EventSubscriber.retryPolicy()` supports programmatic subscribers.
- Local processing retry loop in `UnifiedEventDispatcher`: `maxRetries` is the total budget, `localMaxRetries` controls how many retries run in-process, `localMaxRetries = 0` means durable-only, and `localMaxRetries = maxRetries` means local-only. Processing retries are opt-in; the shipped policy uses 3 local retries followed by 12 durable retries, spanning at least 40 minutes. Retries are asynchronous, jittered and bounded by a global local-attempt cap.
- Configurable processing policy via `event-flow.dispatcher.lifecycle.retry.*`: the top-level settings are the default policy, named overrides are supported, and `total-retries` / `local-max-retries` split the budget between the local and durable layers. The publisher-side durable execution is configured independently via `event-flow.publisher.lifecycle.retry.processing.*`.
- `FailureAck.retryAfterMs` and `FailureAck.policyName`, plus `AcceptedAck.maxProcessingMs`, carry the listener's retry decision to the publisher.
- `HandlerResult` / `HandlerResults` now expose `failureKind`, `retryable`, `attempts`, `policyName` and `retryAfterMs`.
- Durable retry storage: the `event_retry` table (keyed by `event_id`, `retry_kind`) and the `EventRetryStore` SPI with JDBC and in-memory implementations. The table is created automatically when schema initialization is enabled.
- Transactional `LifecycleRepository`: coordinated `event_store` + `event_retry` writes with rollback on either side (`JdbcLifecycleRepository`, `InMemoryLifecycleRepository`).
- `DeliveryRetryScheduler` and `ProcessingRetryScheduler`: durable retries with lease-based claims, attempt counters, exponential backoff, jitter and hard caps. A shared `RetrySchedulerSupport` groups both retry kinds by `eventId` so an event is re-published once per cycle.
- `RetryWatchdog`: raises `DELIVERY` rows for stale `PUBLISHED` events and `PROCESSING` rows for stale `ACCEPTED` events.
- Retry-aware `AckHandler`: routes all four acks through `LifecycleRepository`, applies stale-ack guards by `retryKind` and `attempt`, persists `retryAfterMs`/`policyName`, and supports delivery-only mode.
- Retry-aware `EventLifecyclePublisher`: creates a `DELIVERY` retry row on publish failure, supports `require-accepted-ack = false` (publish success equals `ACCEPTED`), and never rolls back terminal statuses.
- Spring properties split by ownership: publisher-side `event-flow.publisher.lifecycle.retry.delivery.*` / `.processing.*` (shared interval, ack-timeout, batch-size, table) and dispatcher-side `event-flow.dispatcher.lifecycle.retry.*` (processing policy). The publisher no longer reads the dispatcher policy; it enforces its own `processing.max-attempts` safety cap.
- Manual resurrection of `DEAD` events via `markForRetry(eventId)`, which resets the retry row to `PENDING` and the event to `NEW` atomically.
- Retry-aware cleanup: events with `PENDING`/`IN_PROGRESS` retry rows are never deleted, and dead-lettered events use a separate `dead-letter-max-age` retention.

### Changed

- `EventLifecycleDispatcher` now publishes `AcceptedAck` before handler execution and `SuccessAck` / `FailureAck` / `NoHandlerAck` afterwards.
- `AckHandler` handles all four lifecycle acks through conditional transitions. Late acks cannot roll back `HANDLED` or `DEAD`, and a late `AcceptedAck` after a processing failure keeps `FAILED`.
- Lifecycle ack events (`AcceptedAck`, `NoHandlerAck`, `SuccessAck`, `FailureAck`) are excluded from structured logs by default.
- `SuccessAck` and `FailureAck` carry `retryKind` and `attempt`, identifying the retry context of the dispatch that produced the ack.
- `UnifiedEventDispatcher` retries only the failing handler locally; other handlers of the same event are not re-run.
- `ListenerRetryPolicyResolver` centralizes policy declaration, named-reference resolution, default-policy fallback and fail-fast validation. `SpringEventListenerRegistry` performs a two-phase policy scan so cross-bean references work.
- `EventDispatcherBuilder` exposes `retryDelayScheduler(...)` and `localRetryCap(...)`.
- `AckHandler` now defaults to the legacy status-only constructor when no `LifecycleRepository` bean is present, preserving backward compatibility.
- The legacy `EventRetryScheduler` is no longer auto-configured; the two-domain durable schedulers replace it.
- `EventCleanupScheduler` accepts an optional `EventRetryStore` and honours dead-letter retention; the legacy constructor is retained.

### Fixed

- `IdempotentEventDispatcher` now invalidates its cache on any handler failure, not only when all handlers fail. A partial failure is therefore eligible for a processing retry instead of being dropped as a duplicate.
- `EventStatus` status comments in all SQL dialects and `SchemaInitializer` now document the new `A` and `D` codes.
- Retry rows now persist the publish error type and message instead of dropping them.
- Delivery attempt numbers now count from the first publish, so stale acks from the original attempt cannot clear a newer delivery retry.

## [1.3.1] - 2026-10-19

### Changed

- Upgrade `zstd-jni` (transitive via `kafka-clients`) from 1.5.6-4 to 1.5.7-24 to fix native-memory vulnerabilities: out-of-bounds reads, use-after-free, and missing bounds checks in dictionary and stream APIs (CVE-2026-87795, CVE-2026-87823, CVE-2026-87824, CVE-2026-87825, CVE-2026-87877, CVE-2026-89045, CVE-2026-89046, CVE-2026-90560, CVE-2026-90852)

## [1.3.0] - 2026-08-12

### Added

- Local routing metadata: explicit envelope channels are now persisted in the `channels` column of the event store, so a retry republishes the event on the originally requested channels even when the consumer does not know the envelope's channel contract. The wire format of the envelope is unchanged.
- Dialect-aware BOOLEAN column DDL for Oracle and SQL Server: the schema initializer now emits `BOOLEAN` columns via the dialect layer instead of hard-coded `BIT`/`NUMBER` types, so `is_manual`-style flags are created correctly on every supported database.

### Changed

- New installations get the `channels` column automatically from the DDL scripts. Existing installations must migrate the event store table manually:

```sql
-- PostgreSQL / H2 / MySQL
ALTER TABLE event_store ADD COLUMN channels TEXT NULL;

-- Oracle
ALTER TABLE event_store ADD (channels CLOB NULL);

-- SQL Server
ALTER TABLE event_store ADD channels NVARCHAR(MAX) NULL;
```

- Log-levels config now accepts fully-qualified class names as keys (e.g. `io.example.HeartbeatEvent`) in addition to simple class names, and entries with unknown/invalid keys are rejected at startup with `IllegalArgumentException` instead of failing silently at runtime.

### Fixed

- `Envelope` now rejects another `Envelope` as payload, preventing double-wrapping
- `EventRetryScheduler` now requires a service name so retries are limited to events owned by the current service
- Manual retries no longer increment the retry count: the counter reflects only automatic retry attempts, so operator-initiated retries do not consume the `maxRetries` budget or inflate the exponential backoff
- Manual retries now bypass the updated-at cutoff: an event explicitly marked for retry is retried even when it has been recently updated, so operators can force re-delivery without waiting for the staleness window
- Stored timestamps are now read in UTC: previously `StoredEvent` instants could be shifted by the local timezone, which distorted retry/cleanup age calculations
- Spring: lifecycle persistence is deferred until after transaction commit, so the event store is not written when the surrounding transaction rolls back
- Spring: the publication future now completes exceptionally on transaction rollback instead of succeeding silently while no event was actually published

## [1.2.3] - 2026-08-06

### Fixed

- `Envelope.channels()` now honors the `channels()` override when the payload implements the `Event` interface and has no `@Event` annotation; previously the payload's channel list was silently lost and the event fell back to the internal channel

## [1.2.2] - 2026-08-04

### Changed

- Upgrade Jackson to 2.22.1 (`jackson-bom` 2.18.2 → 2.22.1) to fix known vulnerabilities in `jackson-databind` (CVE-2026-54512–54518, CVE-2026-59888, CVE-2026-59889)

## [1.2.1] - 2026-08-02

### Changed

- Upgrade Spring Boot from 3.5.14 to 3.5.16
- Upgrade `commons-lang3` from 3.18.0 to 3.20.0
- Upgrade `jackson-dataformat-msgpack` from 0.9.11 to 0.9.12

## [1.2.0] - 2026-07-12

### Added

- Lifecycle tracking with persistent storage, status tracking, and acknowledgment-based monitoring
- `EventLifecyclePublisher` and `EventLifecycleDispatcher` for end-to-end event tracking
- `EventStore` interface with `JdbcEventStore` (PostgreSQL, H2, MySQL) and `InMemoryEventStore`
- Automatic retry of failed events with exponential backoff
- Cleanup scheduler for old terminal events
- Per-event log level overrides in structured logging
- Manual retry mechanism via `markForRetry()`
- Cross-database DDL scripts and dialect support

## [1.1.0] - 2025-01-01

### Added

- Initial stable release with core event-driven architecture
- `EventPublisher` and `EventDispatcher` with builder pattern
- Structured logging, retry mechanism, transports, serialization
- Spring Boot auto-configuration and transactional publishing

[1.3.1]: https://github.com/vovten/event-flow/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/vovten/event-flow/compare/v1.2.3...v1.3.0
[1.2.3]: https://github.com/vovten/event-flow/compare/v1.2.2...v1.2.3
[1.2.2]: https://github.com/vovten/event-flow/compare/v1.2.1...v1.2.2
[1.2.1]: https://github.com/vovten/event-flow/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/vovten/event-flow/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/vovten/event-flow/releases/tag/v1.1.0
