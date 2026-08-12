# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[1.3.0]: https://github.com/vovten/event-flow/compare/v1.2.3...v1.3.0
[1.2.3]: https://github.com/vovten/event-flow/compare/v1.2.2...v1.2.3
[1.2.2]: https://github.com/vovten/event-flow/compare/v1.2.1...v1.2.2
[1.2.1]: https://github.com/vovten/event-flow/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/vovten/event-flow/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/vovten/event-flow/releases/tag/v1.1.0
