# Spring Integration Specification

## Purpose

The Spring Boot module provides auto-configuration for Event Flow: property-driven setup of publishers, dispatchers, channels, transports, lifecycle tracking, and handler registries. This spec defines the auto-configuration behavior, the configuration property surface, transactional publishing, and Spring-aware handler discovery.

## Requirements

### Requirement: Feature Flag
Event Flow auto-configuration SHALL be disabled by default and SHALL activate only when the `event-flow.enabled` property is set to `true`. Auto-configuration SHALL only activate when the core `EventPublisher` class is present on the classpath.

#### Scenario: Disabled by default
- GIVEN an application with Event Flow on the classpath and no `event-flow.enabled` property
- WHEN the application starts
- THEN no Event Flow beans SHALL be created
- AND a startup hint SHALL be logged with the minimal configuration to enable it

#### Scenario: Enabled explicitly
- GIVEN an application with `event-flow.enabled=true`
- WHEN the application starts
- THEN the Event Flow auto-configuration SHALL activate

### Requirement: Publisher Configuration
The system SHALL create an `EventPublisher` bean when `event-flow.publisher.enabled=true`. Publisher behavior SHALL be configurable: retry with exponential backoff, structured logging, lifecycle-aware persistence, and transactional publishing.

#### Scenario: Publisher bean creation
- GIVEN `event-flow.publisher.enabled=true` with at least one configured channel and transport
- WHEN the application starts
- THEN an `EventPublisher` bean SHALL be available

#### Scenario: Retry decorator
- GIVEN `event-flow.publisher.retry.enabled=true` with configured attempts, initial delay, and multiplier
- WHEN the publisher bean is created
- THEN the publisher SHALL retry failed publications according to the configured schedule

#### Scenario: Logging decorator
- GIVEN `event-flow.publisher.logging.enabled=true`
- WHEN the publisher bean is created
- THEN publications SHALL emit structured JSON logs with the configured payload length limit and excluded event types

#### Scenario: Transactional publishing default
- GIVEN a publisher with no explicit transactional setting
- WHEN the publisher bean is created
- THEN publication SHALL be deferred until the surrounding transaction commits

#### Scenario: Empty channels warn
- GIVEN `event-flow.publisher.enabled=true` with no channels configured
- WHEN the application starts
- THEN a warning SHALL be logged
- AND no publisher bean SHALL be created

### Requirement: Transactional Publishing
The system SHALL provide a transactional publisher decorator that defers publication until the Spring transaction commits. When no transaction is active, publication SHALL occur immediately. When the transaction rolls back, the event SHALL NOT be published and the returned future SHALL complete exceptionally.

#### Scenario: Deferred publication
- GIVEN an active Spring transaction and a transactional publisher
- WHEN an event is published inside the transaction
- THEN the origin publication SHALL NOT occur until the transaction commits
- AND after commit the publication SHALL proceed

#### Scenario: No active transaction
- GIVEN a transactional publisher and no active transaction
- WHEN an event is published
- THEN the origin publication SHALL occur immediately

#### Scenario: Rollback aborts publication
- GIVEN an active Spring transaction and a transactional publisher
- WHEN an event is published and the transaction rolls back
- THEN the event SHALL NOT be published
- AND the returned future SHALL complete exceptionally with a message stating the transaction was not committed

#### Scenario: Outbox guarantee
- GIVEN transactional publishing and lifecycle-aware persistence both enabled
- WHEN an event is published inside a transaction
- THEN the event SHALL be persisted and published only after the transaction commits

### Requirement: Channel Configuration
The system SHALL create one event channel bean per configured channel. Channel names SHALL map to channel types: `internal`, `external`, and `broadcast`, with custom names producing generic channels.

#### Scenario: Internal channel
- GIVEN a channel configured with name `internal` and a transport
- WHEN the channel bean is created
- THEN an internal event channel SHALL be produced

#### Scenario: Channel requires transport
- GIVEN a channel configured without any transport
- WHEN the application starts
- THEN an `IllegalArgumentException` SHALL be thrown

#### Scenario: Unknown transport type
- GIVEN a channel configured with an unsupported transport type
- WHEN the application starts
- THEN an `IllegalStateException` SHALL be thrown
- AND the exception SHALL list the supported transport types

### Requirement: Dispatcher Configuration
The system SHALL create an `EventDispatcher` bean when `event-flow.dispatcher.enabled=true`. The dispatcher SHALL use a virtual-thread executor, configured incoming transports, and an optional concurrency limit. Idempotent deduplication and dispatch logging SHALL be configurable.

#### Scenario: Dispatcher bean creation
- GIVEN `event-flow.dispatcher.enabled=true` with listener packages and at least one incoming transport
- WHEN the application starts
- THEN an `EventDispatcher` bean SHALL be created
- AND the dispatcher SHALL start consuming from its transports

#### Scenario: Listener packages required
- GIVEN `event-flow.dispatcher.enabled=true` without configured listener packages
- WHEN the application starts
- THEN an `IllegalStateException` SHALL be thrown

#### Scenario: Dispatcher requires transport
- GIVEN `event-flow.dispatcher.enabled=true` without incoming transports
- WHEN the application starts
- THEN a warning SHALL be logged
- AND no dispatcher bean SHALL be created

#### Scenario: Concurrency limit
- GIVEN `event-flow.dispatcher.thread-pool.concurrency-limit` set to a positive value
- WHEN the dispatcher bean is created
- THEN concurrent handler execution SHALL be capped at that limit

#### Scenario: Idempotent deduplication
- GIVEN `event-flow.dispatcher.idempotent.enabled=true`
- WHEN the dispatcher bean is created
- THEN duplicate event delivery SHALL be prevented within the configured TTL and cache size

#### Scenario: Virtual thread executor
- GIVEN dispatcher configuration enabled
- WHEN the executor bean is created
- THEN it SHALL be a virtual-thread-per-task executor

### Requirement: Handler Registry Discovery
The system SHALL discover handlers from the Spring application context: beans with `@EventListener` methods within a configured scan package and beans implementing the `EventSubscriber` interface. Discovery SHALL respect bean proxies and SHALL rescan after context refresh.

#### Scenario: Annotation listener discovery
- GIVEN a Spring bean with an `@EventListener` method in the scan package
- WHEN the application starts
- THEN the method SHALL be registered as a handler

#### Scenario: Interface subscriber discovery
- GIVEN a Spring bean implementing `EventSubscriber`
- WHEN the application starts
- THEN the bean SHALL be registered as a handler for its declared event types

#### Scenario: Proxy-safe discovery
- GIVEN a Spring bean with an `@EventListener` method that is proxied by a framework
- WHEN the handler is registered
- THEN the underlying user class SHALL be inspected rather than the proxy

#### Scenario: Package name validation
- GIVEN a dispatcher listener package that is not a valid Java package name
- WHEN the application starts
- THEN an `IllegalArgumentException` SHALL be thrown

### Requirement: Serialization Auto-Configuration
The system SHALL create an `EventSerializerFactory` bean and register all custom serializer beans into it. The system SHALL register the configured allowed event packages into the deserialization whitelist. Default allowed packages SHALL include `io.github.vovten.eventflow`.

#### Scenario: Default serializers
- GIVEN auto-configuration active
- WHEN the serializer factory bean is created
- THEN the JSON and MessagePack serializers SHALL be registered

#### Scenario: Custom serializer registration
- GIVEN a custom serializer bean in the context
- WHEN the serializer factory bean is created
- THEN the custom serializer SHALL be registered by its name and code

#### Scenario: Deserialization whitelist
- GIVEN `event-flow.dispatcher.deserialization.allowed-event-packages` configured
- WHEN the application starts
- THEN those packages SHALL be added to the deserialization whitelist

### Requirement: Lifecycle Auto-Configuration
The system SHALL create lifecycle beans when `event-flow.publisher.lifecycle.enabled=true`: an event store, an acknowledgement handler, an optional retry scheduler, and an optional cleanup scheduler. A service name SHALL be required.

#### Scenario: Event store selection
- GIVEN lifecycle enabled with `store.type=in-memory`
- WHEN the event store bean is created
- THEN an in-memory event store SHALL be produced

#### Scenario: JDBC store from data source
- GIVEN lifecycle enabled with the default `store.type=db` and a data source present
- WHEN the event store bean is created
- THEN a JDBC event store SHALL be produced using the configured table name

#### Scenario: Missing data source
- GIVEN lifecycle enabled with `store.type=db` and no data source
- WHEN the event store bean is created
- THEN an `IllegalStateException` SHALL be thrown

#### Scenario: Service name required
- GIVEN lifecycle enabled without a service name
- WHEN the acknowledgement handler bean is created
- THEN an `IllegalStateException` SHALL be thrown

#### Scenario: Retry scheduler bean
- GIVEN lifecycle enabled with retry enabled (the default)
- WHEN the application starts
- THEN a retry scheduler SHALL be created with the configured interval, maximum retries, and batch size

#### Scenario: Cleanup scheduler bean
- GIVEN lifecycle enabled with cleanup enabled
- WHEN the application starts
- THEN a cleanup scheduler SHALL be created with the configured interval, maximum age, and batch size

### Requirement: Spring Handler Registry Builder
The system SHALL provide a Spring-aware registry builder that discovers annotation listeners in a scan package and interface subscribers from the application context, and composes them into a single registry.

#### Scenario: Build composite registry
- GIVEN a builder with both annotation and interface listeners enabled
- WHEN the registry is built
- THEN a composite registry SHALL be produced containing both discovery registries

#### Scenario: Builder requires a source
- GIVEN a builder with no scan package, no interface listeners, and no custom registries
- WHEN the registry is built
- THEN an `IllegalStateException` SHALL be thrown

#### Scenario: Null context rejected
- GIVEN a builder created with a null application context
- WHEN it is created
- THEN an `IllegalArgumentException` SHALL be thrown
