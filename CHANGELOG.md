# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[1.2.0]: https://github.com/vovten/event-flow/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/vovten/event-flow/releases/tag/v1.1.0
