# Configuration

## Purpose

Define how the Spring integration assembles the Event Flow components through auto-configuration driven by properties: enabling and disabling modules, configuring the publisher and dispatcher, wiring channels, transports, serializers, handler registries, and lifecycle tracking, all through the `event-flow` configuration namespace.

## Requirements

### Requirement: Module enablement

The framework's Spring auto-configuration is disabled by default and turned on explicitly, with independent control over its major modules.

#### Scenario: Enabling the framework

- **WHEN** application configuration sets `event-flow.enabled=true`
- **THEN** the Event Flow auto-configuration is activated and its principal beans are configured

#### Scenario: Leaving the framework disabled

- **WHEN** application configuration does not enable the framework
- **THEN** no Event Flow auto-configuration is applied

#### Scenario: Enabling the publisher independently

- **WHEN** application configuration enables the publisher module
- **THEN** an `EventPublisher` bean is configured with the declared channels, retry, and transactional settings

#### Scenario: Enabling the dispatcher independently

- **WHEN** application configuration enables the dispatcher module
- **THEN** an `EventDispatcher` bean is configured with the declared listener packages, transports, and processing options

### Requirement: Conditional assembly

Auto-configuration activates each component only when the conditions for it are satisfied, so modules compose without redundancy and without overriding user-provided beans.

#### Scenario: Honouring user-provided beans

- **WHEN** an application defines its own bean for a component that auto-configuration would otherwise create
- **THEN** the user-provided bean is honoured instead of being replaced

#### Scenario: Depending on the presence of the core library

- **WHEN** the core library classes are not present on the classpath
- **THEN** auto-configuration does not activate

### Requirement: Publisher configuration

The publisher's behaviour is fully configurable through properties, including retry, transactional handling, and logging.

#### Scenario: Configuring retry

- **WHEN** retry is enabled for the publisher and its maximum attempts, initial delay, and backoff multiplier are set
- **THEN** the configured publisher applies those retry settings

#### Scenario: Configuring transactional publishing

- **WHEN** transactional publishing is enabled for the publisher
- **THEN** events are published only after the surrounding transaction commits

#### Scenario: Configuring published-event logging

- **WHEN** publisher logging is enabled
- **THEN** published events are logged with the configured payload length limit and excluded event types

### Requirement: Channel configuration

Channels and their transports are declared declaratively, with built-in support for local-queue, Kafka, and broadcast Kafka transports.

#### Scenario: Declaring an internal channel

- **WHEN** a channel of the internal type is declared with a local-queue transport
- **THEN** a corresponding channel bean is created using that transport

#### Scenario: Declaring an external channel

- **WHEN** a channel of the external type is declared with a Kafka transport
- **THEN** a corresponding channel bean is created using that transport with the configured servers and topics

#### Scenario: Declaring a broadcast channel

- **WHEN** a broadcast channel is declared with a broadcast-Kafka transport
- **THEN** a corresponding channel bean is created that broadcasts to all configured partitions

### Requirement: Dispatcher configuration

The dispatcher's behaviour is configurable, including listener scanning, thread pool, concurrency, idempotency, and logging.

#### Scenario: Configuring listener scanning

- **WHEN** listener packages are configured for the dispatcher
- **THEN** handlers are discovered from those packages when the dispatcher is assembled

#### Scenario: Configuring idempotent processing

- **WHEN** idempotency is enabled for the dispatcher
- **THEN** the dispatcher deduplicates already-processed events within the configured TTL and size bounds

#### Scenario: Configuring deserialization allow-listing

- **WHEN** allowed event packages are configured for the dispatcher
- **THEN** only events from those packages are deserialized for dispatch

#### Scenario: Configuring concurrency

- **WHEN** a concurrency limit is configured for the dispatcher
- **THEN** handler executions are bounded by that limit

### Requirement: Serializer configuration

Custom serializers declared as beans are registered with the serializer factory so they are available to the framework.

#### Scenario: Registering a custom serializer

- **WHEN** an application declares a custom serializer bean
- **THEN** the serializer is registered with the serializer factory and becomes available by its name and code

### Requirement: Lifecycle configuration

Lifecycle-aware publishing and tracking are enabled through properties and assembled from the event store, ack handling, and scheduled retry and cleanup.

#### Scenario: Enabling lifecycle tracking

- **WHEN** lifecycle tracking is enabled and a service name is configured
- **THEN** the publisher becomes lifecycle-aware and events are persisted and tracked

#### Scenario: Selecting the event store type

- **WHEN** the lifecycle store type is set to database or in-memory
- **THEN** the corresponding event store bean is created

#### Scenario: Requiring a service name for lifecycle tracking

- **WHEN** lifecycle tracking is enabled but no service name is configured
- **THEN** auto-configuration fails with a clear error explaining that the service name is required

#### Scenario: Enabling retry and cleanup schedulers

- **WHEN** lifecycle retry or cleanup is enabled
- **THEN** the corresponding scheduled components are created and started to retry failed events and clean up old terminal events

### Requirement: Configuration validation

Configuration errors are surfaced clearly at startup rather than failing silently at runtime.

#### Scenario: Failing fast on invalid configuration

- **WHEN** required settings for an enabled module are missing or invalid
- **THEN** startup fails with a descriptive exception naming the missing or invalid configuration