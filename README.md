# Event Flow

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java](https://img.shields.io/badge/Java-21-blue)](https://openjdk.java.net/)

**Event Flow** is a lightweight Java framework for building event-driven applications. It provides the structural backbone for publishing, routing, and processing events — so you can focus on business logic instead of wiring infrastructure.

## 📖 Table of Contents

- [Features](#-features)
- [Architecture](#-architecture)
- [Modules](#-modules)
- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Core Components](#-core-components)
- [Lifecycle Tracking](#-lifecycle-tracking)
- [Usage Examples](#-usage-examples)
- [Configuration](#-configuration)
- [Interaction Diagrams](#-interaction-diagrams)

## ✨ Features

- **Flexible Routing** — Event channels with configurable transports
- **Multiple Transports** — LocalQueue (in-JVM) and Apache Kafka out of the box, with extension points for custom transports
- **Annotation-Based** — Event handling via `@EventListener`
- **Interface-Based** — Event handling via `EventSubscriber` interface
- **POJO/Record Events** — Support for plain Java objects without `Event` interface
- **Idempotency** — Event deduplication based on UID
- **Transactional Publishing** — Send events after transaction commit
- **Structured Logging** — Decorators for publisher and dispatcher with machine-parseable JSON output
- **Retry Mechanism** — Exponential backoff with configurable parameters
- **Lifecycle Tracking** — End-to-end event lifecycle with persistent storage, status tracking, automatic retry of failed events, and acknowledgment-based monitoring
- **Extensible Serialization** — JSON and MessagePack with support for custom formats

## 🏗 Architecture

### High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Event Flow Architecture                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐         ┌─────────────────────────────────────────┐   │
│  │   Service    │────────▶│           Event Channels                │   │
│  │  (Publisher) │         │  ┌─────────────┐  ┌─────────────────┐   │   │
│  └──────────────┘         │  │  Internal   │  │    External     │   │   │
│                           │  │  (in-JVM)   │  │    (Kafka)      │   │   │
│                           │  └──────┬──────┘  └────────┬────────┘   │   │
│                           │         │                  │            │   │
│                           └─────────┼───────────────── ┼────────────┘   │
│                                     │                  │                │
│                           ┌─────────▼───────────────── ▼────────────┐   │
│                           │     Outgoing Event Transports           │   │
│                           │  ┌─────────────┐  ┌─────────────────┐   │   │
│                           │  │ LocalQueue  │  │      Kafka      │   │   │
│                           │  │   Queue     │  │    Producer     │   │   │
│                           │  └─────────────┘  └─────────────────┘   │   │
│                           └─────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │  External Communication
                                    │  (Network / Message Broker)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    External Event Flow (Kafka)                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│                           ┌─────────────────────────────────────────┐   │
│                           │      Incoming Event Transports          │   │
│                           │  ┌─────────────┐  ┌─────────────────┐   │   │
│                           │  │ LocalQueue  │  │      Kafka      │   │   │
│                           │  │   Queue     │  │    Consumer     │   │   │
│                           │  └─────────────┘  └─────────────────┘   │   │
│                           └─────────┬─────────────────┬─────────────┘   │
│                                     │                 │                 │
│                           ┌─────────▼─────────────────▼────────────┐    │
│                           │           Event Dispatcher             │    │
│                           │                                        │    │
│                           └─────────┬─────────────────┬────────────┘    │
│                                     │                 │                 │
│                           ┌─────────▼─────────────────▼────────────┐    │
│                           │           Handler Registry             │    │
│                           │  ┌─────────────┐  ┌─────────────────┐  │    │
│                           │  │ Annotation  │  │   Interface     │  │    │
│                           │  │   Based     │  │     Based       │  │    │
│                           │  └─────────────┘  └─────────────────┘  │    │
│                           └─────────┬─────────────────┬────────────┘    │
│                                     │                 │                 │
│                           ┌─────────▼─────────────────▼────────────┐    │
│                           │        Event Handlers                  │    │
│                           │   (@EventListener / EventSubscriber)   │    │
│                           └────────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Microservices Communication

```
┌────────────────────────────────────────────────────────────────────────────┐
│                     Microservice A (Event Producer)                        │
│                                                                            │
│   ┌──────────┐   ┌───────────┐   ┌──────────┐   ┌──────────────────────┐   │
│   │ Service  │──▶│ Publisher │──▶│ Channel  │──▶│ KafkaOutTransport    │   │
│   └──────────┘   └───────────┘   └──────────┘   └──────────┬───────────┘   │
└────────────────────────────────────────────────────────────┼───────────────┘
                                                             │ produce()
                                                             ▼
               ═══════════════════════════════════════════════════
                    Apache Kafka — Topic: "events" 
               ═══════════════════════════════════════════════════
                ┃                                              ┃
     consume()  ┃                                   consume()  ┃
       │        ┃                                     │        ┃
       ▼        ┃                                     ▼        ┃
┌─────────────────────────────────────┐ ┌─────────────────────────────────────┐
│        Microservice B (Consumer)    │ │        Microservice C (Consumer)    │
│                                     │ │                                     │
│  ┌──────────────────┐               │ │    ┌──────────────────┐             │
│  │ KafkaInTransport │               │ │    │ KafkaInTransport │             │
│  └──────────────────┘               │ │    └──────────────────┘             │
│           │                         │ │             │                       │
│  ┌────────▼─────────┐               │ │    ┌────────▼─────────┐             │
│  │ EventDispatcher  │               │ │    │ EventDispatcher  │             │
│  └────────┬─────────┘               │ │    └────────┬─────────┘             │
│           │ dispatch()              │ │             │ dispatch()            │
│           │                         │ │             │                       │
│  ┌────────▼─────────┐               │ │    ┌────────▼────────┐              │
│  │ HandlerRegistry  │               │ │    │ HandlerRegistry │              │
│  └────────┬─────────┘               │ │    └────────┬────────┘              │
│           │ getHandlers()           │ │             │ getHandlers()         │
│           ▼                         │ │             ▼                       │
│  ┌──────────────────┐               │ │    ┌──────────────────┐             │
│  │    Handler 1     │               │ │    │    Handler 2     │             │
│  │    Handler 2     │               │ │    │    Handler 3     │             │
│  └──────────────────┘               │ │    └──────────────────┘             │
└─────────────────────────────────────┘ └─────────────────────────────────────┘
```

### Single Application Flow (Internal Events)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Single Application                                │
│                                                                             │
│   ┌──────────┐   ┌───────────┐   ┌──────────┐   ┌──────────────────────┐    │
│   │ Service  │──▶│ Publisher │──▶│ Channel  │──▶│LocalQueueOutTransport│    │
│   └──────────┘   └───────────┘   └──────────┘   └──────────┬───────────┘    │
│                                                            │ offer(event)   │
│                                                            ▼                │
│              ┌───────────────────────────────────────────────────────┐      │
│              │              BlockingDeque<Event>                     │      │
│              │                    (shared queue)                     │      │
│              └─┬─────────────────────────────────────────────────────┘      │
│                │ take(event)                                                │
│                ▼                                                            │
│   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────────┐    │
│   │ LocalQueueInTrans│──▶│ EventDispatcher  │──▶│ EventHandlerRegistry │    │
│   └──────────────────┘   └────────┬─────────┘   └──────────┬───────────┘    │
│                                   │ dispatch()             │ getHandlers()  │
│                                   │                        ▼                │
│                                   │              ┌──────────────────────┐   │
│                                   └─────────────▶│     Handlers         │   │
│                                    async execute │  (virtual threads)   │   │
│                                                  └──────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 📦 Modules

| Module | Description | Documentation |
|--------|-------------|---------------|
| **event-flow-core** | Core module — framework-agnostic, pure Java 21+ | [README](event-flow-core/README.md) |
| **event-flow-spring** | Spring Boot auto-configuration with YAML | [README](event-flow-spring/README.md) |

## 📦 Installation

### Maven

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.vovten</groupId>
    <artifactId>event-flow</artifactId>
    <version>1.2.1</version>
</dependency>
```

For Spring Boot integration:

```xml
<dependency>
    <groupId>io.github.vovten</groupId>
    <artifactId>event-flow-spring</artifactId>
    <version>1.2.1</version>
</dependency>
```

### Gradle

```groovy
implementation 'io.github.vovten:event-flow:1.1.0'
// For Spring Boot:
implementation 'io.github.vovten:event-flow-spring:1.1.0'
```

### Requirements

- Java 21+
- Apache Kafka 3.6.0+ (optional, for external events)

## 🚀 Quick Start

### 1. Create an Event

Events can be defined in two ways:

**a) Use the `@Event` annotation (recommended)** — cleaner POJO/record, channels from annotation:

```java
@Event(channels = InternalEventChannel.class)
public record OrderShipped(String orderId, String customerId) {}
```

**b) Implement the `Event` interface** — full control over type and channels:

```java
public record OrderCreatedEvent(String orderId, String customerId) implements Event {

    @Override
    public Class<?> type() {
        return OrderCreatedEvent.class;
    }
}
```

> Both approaches work with `publish()`, `prepare()`, and handler registration.
> **Recommended:** `@Event` annotation for cleaner code; use `implements Event` only when you need full control.

### 2. Set Up Infrastructure

```java
// Create local queue transport for in-JVM event delivery
var transports = new LocalQueueTransportsBuilder()
    .queueSize(1000)
    .build();

// Create a channel for internal (in-application) events
// For external events (Kafka), use ExternalEventChannel with Kafka transports
EventChannel internalChannel = new InternalEventChannel(
    List.of(transports.publisher())
);

// Create publisher (add externalChannel here for external events)
EventPublisher eventPublisher = EventPublisherBuilder.create(internalChannel)
    .retryable(3, Duration.ofMillis(100), 2.0)
    .buildAndLog();

// Create handler registry
EventHandlerRegistry handlerRegistry = EventHandlerRegistryBuilder.create()
    .withAnnotationListeners()
    .withInterfaceListeners()
    .buildAndLog();

// Create dispatcher
EventDispatcher eventDispatcher = EventDispatcherBuilder.create()
    .executor(Executors.newVirtualThreadPerTaskExecutor())
    .handlerRegistry(handlerRegistry)
    .transports(List.of(transports.dispatcher()))
    .concurrencyLimit(100)
    .buildAndLog();

// Start the dispatcher
eventDispatcher.start(eventDispatcher::dispatch);
```

### 3. Create a Handler (Annotation-Based)

Handle the event directly:

```java
public class OrderEventHandler {

    @EventListener
    public void handleOrderCreated(OrderCreatedEvent event) {
        System.out.println("Order created: " + event.orderId());
    }
}

// Register the handler
handlerRegistry.register(new OrderEventHandler());
```

Or receive the full `Envelope` with metadata (eventId, processId, occurredAt, etc.):

```java
public class OrderEnvelopeHandler {

    @EventListener(OrderShipped.class)
    public void handleOrderShipped(Envelope<OrderShipped> envelope) {
        OrderShipped event = envelope.payload();
        System.out.println("Order " + event.orderId()
                + " processed with id " + envelope.eventId());
    }
}

// Register the envelope handler
handlerRegistry.register(new OrderEnvelopeHandler());
```

> When the handler parameter is `Envelope<T>`, the `@EventListener` annotation must specify the payload type explicitly (e.g., `@EventListener(OrderShipped.class)`).
> This is especially useful for POJO/record events annotated with `@Event` — they are automatically wrapped in an `Envelope` upon publishing.

### 4. Publish an Event

```java
public class OrderService {

    private final EventPublisher eventPublisher;

    public OrderService(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    public void createOrder(String customerId) {
        // Business logic...
        eventPublisher.publish(new OrderCreatedEvent("order-123", customerId));
    }
}
```

## 🔧 Core Components

### Event

Base interface for all events. Defines event type and publication channels.

```java
public interface Event {
    Class<?> type();
    default List<Class<? extends EventChannel>> channels() {
        return List.of(InternalEventChannel.class);
    }
    default String asJson() {
        return EventUtils.toJson(this);
    }
}
```

**TraceableEvent** — extends `Event` with tracing fields: `eventId` (UUID), `processId` (correlation), `occurredAt` (timestamp).

### Envelope

Wrapper for domain events that adds technical metadata. Automatically captures:
- `eventId` (UUID) — unique event identifier
- `processId` (UUID) — correlation ID (e.g., saga ID)
- `occurredAt` (Instant) — event timestamp
- `metadata` (Map) — custom key-value pairs
- `payload` — the actual domain object

The envelope implements `Event` interface, so it passes through existing transport infrastructure.

**Creating Envelopes:**

```java
// Auto-generated metadata
Envelope<OrderCreatedEvent> envelope = Envelope.of(new OrderCreatedEvent("123"));

// With custom processId (correlation)
UUID processId = UUID.fromString("...");
Envelope<OrderCreatedEvent> envelope = Envelope.of(new OrderCreatedEvent("123"), processId);

// With explicit channels
Envelope<OrderCreatedEvent> envelope = Envelope.of(
    new OrderCreatedEvent("123"),
    List.of(ExternalEventChannel.class)
);
```

**Channels from `@Event` Annotation:**
POJO/record classes can use the `@Event` annotation to specify default channels:

```java
@Event(channels = ExternalEventChannel.class)
public record OrderCreatedEvent(String orderId) {}
```

### EventChannel

A channel defines event delivery routes through transports.

```java
public interface EventChannel {
    String name();
    List<OutTransport> transports();
    CompletableFuture<SendResults> send(Event event);
}

```

**Built-in Channels:**
- `InternalEventChannel` — for internal in-application delivery
- `ExternalEventChannel` — for external delivery to other applications/microservices

### EventPublisher

Publishes events to configured channels.

**Creating via Builder:**

```java
EventPublisher publisher = EventPublisherBuilder.create(internalChannel, externalChannel)
    .retryable(3, Duration.ofMillis(100), 2.0)
    .build();
```

**EventPublisherBuilder** — fluent builder for creating publishers with flexible configuration:

| Method | Description |
|--------|-------------|
| `create(...)` | Create builder with event channels (required) |
| `retryable()` | Enable retry with default settings (3 attempts, 100ms initial delay, 2.0 multiplier) |
| `retryable(max, delay, multiplier)` | Enable retry with custom settings |
| `withDecorator(fn)` | Add custom decorator to the publisher chain |
| `build()` | Build the publisher |
| `buildAndLog()` | Build the publisher and log the configuration |
| `loggable()` | Enable structured logging (JSON, 1024 char payload limit) |
| `loggable(maxPayloadLength)` | Enable structured logging with custom payload truncation |
| `loggable(maxPayloadLength, excludedEvents)` | Enable structured logging with event type exclusion |
| `loggable(maxPayloadLength, excludedEvents, logLevels)` | Enable structured logging with per-event log level overrides |

### EventDispatcher

Delivers events from transports to handlers.

```java
public interface EventDispatcher {
    void dispatch(Event event);
    void register(Object listener);
    boolean isRegistered(Object listener);
    void start(Consumer<Event> handler);
    void stop();
}
```

**EventDispatcherBuilder** — fluent builder for creating dispatchers:

| Method | Description |
|--------|-------------|
| `executor(...)` | Configure ExecutorService (required) |
| `handlerRegistry(...)` | Handler registry (required) |
| `transports(...)` | List of incoming transports |
| `concurrencyLimit(n)` | Concurrency limiting via Semaphore |
| `idempotent()` | Enable idempotency (deduplication by UID) |
| `idempotent(maxSize, ttl)` | Enable idempotency with custom settings |
| `withDecorator(fn)` | Add custom decorator |
| `build()` | Build the dispatcher |
| `buildAndLog()` | Build the dispatcher and log the configuration |
| `loggable()` | Enable structured logging (JSON, 1024 char payload limit) |
| `loggable(maxPayloadLength)` | Enable structured logging with custom payload truncation |
| `loggable(maxPayloadLength, excludedEvents)` | Enable structured logging with event type exclusion |
| `loggable(maxPayloadLength, excludedEvents, logLevels)` | Enable structured logging with per-event log level overrides |

### EventHandlerRegistry

Registry for discovering and managing event handlers.

```java
public interface EventHandlerRegistry {
    List<EventHandler> getHandlers(Event event);
    void register(Object listener);
    void unregister(Object listener);
    boolean isRegistered(Object listener);
    void merge(EventHandlerRegistry registry);
    int handlerCount();
    String name();
}
```

**Built-in implementations:**
- `EventListenerRegistry` — discovery via `@EventListener` annotation
- `EventSubscriberRegistry` — discovery via `EventSubscriber` interface
- `CompositeEventHandlerRegistry` — combines multiple registries

**EventHandlerRegistryBuilder** — fluent builder for creating registries:

| Method | Description |
|--------|-------------|
| `withAnnotationListeners()` | Enable discovery via `@EventListener` annotation |
| `withInterfaceListeners()` | Enable discovery via `EventSubscriber` interface |
| `withCustomRegistry(registry)` | Add a custom registry |
| `withDecorator(fn)` | Add a decorator |
| `build()` | Build the registry |
| `buildAndLog()` | Build the registry and log the configuration |

### EventLifecycle

An enum that controls how an event's journey is tracked. Three levels:

| Level | Behaviour |
|-------|-----------|
| `NONE` | Fire-and-forget — event passes through without any persistence |
| `PERSISTED` | Event is stored in the `EventStore` (with `UNDEFINED` status) but not actively tracked |
| `MANAGED` | Full lifecycle tracking — event status transitions through `NEW → PUBLISHED → HANDLED`, with automatic retry on failure |

Set via `@Event(lifecycle = ...)` annotation or the `lifecycle()` default method on `Event`.

### EventStore

Persistence layer for lifecycle tracking. Stores serialised events and tracks their status as they flow through the system.

- **`JdbcEventStore`** — production-grade, backed by a relational database (PostgreSQL, H2, MySQL, etc.)
- **`InMemoryEventStore`** — in-JVM `ConcurrentHashMap`-backed store for testing and single-JVM scenarios

### EventLifecyclePublisher

Decorator for `EventPublisher` that persists events to the `EventStore` before they are sent and updates their status (`NEW → PUBLISHED/FAILED`) after publishing. See the [Lifecycle Tracking](#-lifecycle-tracking) section.

### EventLifecycleDispatcher

Decorator for `EventDispatcher` that publishes `SuccessAck` or `FailureAck` events back to the source channels after handler execution — enabling end-to-end status tracking. See the [Lifecycle Tracking](#-lifecycle-tracking) section.

### EventRetryScheduler

Periodically scans the `EventStore` for failed (`FAILED`), stuck (`PUBLISHED`), and orphaned (`NEW`) events and re-publishes them with exponential backoff. See the [Lifecycle Tracking](#-lifecycle-tracking) section.

### AckHandler

An `EventSubscriber` that processes incoming lifecycle acknowledgment events (`SuccessAck`/`FailureAck`) and updates the event status in the `EventStore`. Filters by service name to allow multiple publisher instances to share the same channel. See the [Lifecycle Tracking](#-lifecycle-tracking) section.

### EventTransport

Transports for event delivery.

**Incoming Transports (`InTransport`):**
- `LocalQueueInTransport` — receive from local queue
- `KafkaInTransport` — receive from Kafka topics

**Outgoing Transports (`OutTransport`):**
- `LocalQueueOutTransport` — send to local queue
- `KafkaOutTransport` — send to Kafka topic
- `BroadcastKafkaOutTransport` — send to all Kafka topic partitions

### LocalQueueTransportsBuilder

Utility for creating paired incoming/outgoing transports based on a local queue:

```java
var transports = new LocalQueueTransportsBuilder()
    .queueSize(1000)
    .build();

EventChannel channel = new InternalEventChannel(
    List.of(transports.publisher())
);

EventDispatcher dispatcher = EventDispatcherBuilder.create()
    .executor(Executors.newVirtualThreadPerTaskExecutor())
    .handlerRegistry(handlerRegistry)
    .transports(List.of(transports.dispatcher()))
    .build();

dispatcher.start(dispatcher::dispatch);
```

### Serialization

**EventSerializer** — serialization interface with magic byte prefix:
- `0x01` — JSON
- `0x02` — MessagePack

**EventSerializerFactory** — factory with automatic format detection:

```java
// Register a custom serializer
EventSerializerFactory.getInstance().register(new MyCustomSerializer());
```

**EventTypeRegistry** — security whitelist for allowed event classes:

```java
// Allow a package (default: io.github.vovten.eventflow.*)
EventTypeRegistry.allowPackage("com.example.events");

// Allow a specific class
EventTypeRegistry.allowClass(MyEvent.class);
```

---

## 🔄 Lifecycle Tracking

Event Flow provides an end-to-end **lifecycle tracking** system that answers three questions about every event:

- **Was it saved?** — the event persisted before publication (crash recovery)
- **Was it delivered?** — the event reached all target channels successfully
- **Was it handled?** — all registered handlers processed the event without errors

This turns event publishing from a fire-and-forget operation into a **reliable, observable process** — essential for critical business events (orders, payments, notifications) where you need guarantees and visibility.

### When you need it

| Scenario | Without lifecycle | With lifecycle (`MANAGED`) |
|----------|------------------|---------------------------|
| Service crashes mid-publish | Event lost | Event safe in store, retried on restart |
| Handler throws an exception | Silent failure | Status updated to `FAILED`, automatic retry |
| Ack lost in transit | Event stuck in limbo | Detected as `PUBLISHED` → retried |
| Debugging production issues | Logs only | Queryable event store with full history |

You choose the level per event — `NONE` for high-throughput fire-and-forget, `PERSISTED` for audit without monitoring, `MANAGED` when you need guarantees.

### How it works

```
┌─────────────────────────── PUBLISHER ────────────────────────────┐
│                                                                  │
│  Event ──► EventLifecyclePublisher ──► EventStore ──► Channel    │
│                    │                         │                   │
│                    │  saves & tracks status  │                   │
│                    │  (NEW→PUBLISHED/FAILED) │                   │
│                    │                         │                   │
│  ◄── EventRetryScheduler scans FAILED, PUBLISHED, NEW ──► retry  │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
         │                                  ▲
         │         SuccessAck / FailureAck  │
         ▼                                  │
┌────────────────────────── DISPATCHER ────────────────────────────┐
│                                                                  │
│  Channel ──► EventLifecycleDispatcher ──► Handlers               │
│                      │                                           │
│                      │  publishes ack after handler execution    │
│                                                                  │
│  ◄── AckHandler processes ack, updates store (HANDLED/FAILED)    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

**The flow:**
1. `EventLifecyclePublisher` saves the event to `EventStore` with status `NEW`, then publishes it
2. On successful delivery, status updates to `PUBLISHED`; on failure, to `FAILED`
3. `EventLifecycleDispatcher` (on the consumer side) publishes `SuccessAck` or `FailureAck` back through the channel
4. `AckHandler` (on the publisher side) catches the ack and updates status to `HANDLED` or `FAILED`
5. `EventRetryScheduler` periodically rescans `FAILED`, stuck `PUBLISHED`, and orphaned `NEW` events and retries them with exponential backoff

### Three lifecycle levels

| Level | Behaviour | Use case |
|-------|-----------|----------|
| `NONE` | No persistence, no tracking. Event passes straight through | High-throughput notifications, ephemeral events |
| `PERSISTED` | Event saved to store but status stays `UNDEFINED`. No retry, no ack | Audit trail without operational guarantees |
| `MANAGED` | Full tracking: `NEW → PUBLISHED → HANDLED`, automatic retry on failure, ack-based end-to-end confirmation | Orders, payments, critical business events |

Set via `@Event` annotation:

```java
@Event(channels = InternalEventChannel.class, lifecycle = EventLifecycle.MANAGED)
public record OrderCreated(String orderId) {}
```

Or via `Event.lifecycle()` default method — the annotation takes precedence when both are present.

### Configuration

Enable lifecycle tracking in Spring Boot with three properties:

```yaml
event-flow:
  publisher:
    lifecycle:
      enabled: true              # Enable lifecycle-aware publishing
      service-name: order-service # Required! Identifies this service for ack filtering
      store:
        type: db                 # "db" (PostgreSQL/H2/MySQL) or "in-memory" (testing)
        table-name: event_store
        auto-init-schema: true    # Disable in production, manage DDL via Flyway
      retry:
        enabled: true
        max-retries: 3
        retry-interval: 30s
        min-age: 30s              # Backoff: 30s, then 60s, then 120s
  dispatcher:
    lifecycle:
      enabled: true               # Enable ack generation on the dispatcher side
```

The `service-name` is mandatory — it identifies this service instance so that `AckHandler` only processes acknowledgments meant for it (multiple services can share the same ack channel without interference).

### Retry mechanism

`EventRetryScheduler` scans for three categories of events that need retry:

| Status | Why it happens |
|--------|---------------|
| `FAILED` | Publication or handler threw an error |
| `PUBLISHED` | Event was sent but ack was lost in transit (stuck) |
| `NEW` | Event was saved but the application crashed before publishing finished (orphaned) |

Backoff is exponential: `delay = minAge × 2^retryCount`. First retry at 30s, second at 60s, third at 120s.

### Cleanup

`EventCleanupScheduler` periodically deletes old terminal events (`HANDLED` and `UNDEFINED`) from the `EventStore` to prevent unbounded growth. Deletion is performed in configurable batches with pauses between batches to reduce database load.

```yaml
event-flow:
  publisher:
    lifecycle:
      cleanup:
        enabled: true
        max-age: 7d           # Events older than this are deleted
        batch-size: 500       # Rows per DELETE
        interval: 60m         # How often the scheduler runs
        pause-between-batches: 100ms  # Throttle between batches
```

**Safety:**
- Only terminal statuses (`HANDLED`, `UNDEFINED`) are cleaned up — `FAILED` events are preserved for manual inspection
- A single cycle deletes at most 100 000 events; leftover events are picked up by the next cycle
- A random jitter (up to `interval`) is added to the first run to avoid thundering herd when multiple instances start

### Storage

The `EventStore` interface has two built-in implementations:

- **`JdbcEventStore`** — production grade, backed by a relational database. Table is created automatically by default. Dialect-specific DDL scripts are shipped at `io/github/vovten/eventflow/lifecycle/store/db/event-store-<dialect>.sql` for manual migration tooling.
- **`InMemoryEventStore`** — `ConcurrentHashMap`-backed, for testing and single-JVM scenarios where persistence is not needed.

You can also implement `EventStore` with your own backend (Redis, MongoDB, etc.) and configure it via `store.type`.

---

## 📝 Usage Examples

### Publishing Options Overview

Event Flow provides multiple ways to publish events:

| Method | Envelope | Channels | Metadata | Message Size |
|--------|----------|----------|----------|--------------|
| `implements Event` | ❌ No | From event | Minimal | ⚡ Smallest |
| `prepare().publish()` | ✅ Auto | Custom | Custom | Medium |

> **Note:** When POJO/record or `prepare()` is used, an `Envelope` is automatically created wrapping the payload with additional metadata (eventId, processId, occurredAt). This increases message size but adds correlation/tracing capabilities.

### Recommended: `@Event` Annotation + `prepare()` Builder

The most convenient approach for most use cases:

```java
// Define event with default channels via annotation
@Event(channels = {InternalEventChannel.class, ExternalEventChannel.class})
public record OrderCreatedEvent(long orderId) {}

// Publish with custom metadata (channels from annotation are used automatically)
eventPublisher.prepare(new OrderCreatedEvent(1))
    .withProcessId(processId)
    .publish();
```

This gives you:
- Channels from `@Event` annotation (no need to specify in code)
- Auto-generated eventId and occurredAt (timestamps)
- Custom metadata via builder (processId, etc.)
- Envelope for correlation/tracing

### 1. Fastest: Event Interface (No Envelope)

Implement `Event` interface for minimum overhead — no Envelope wrapper, smallest message size:

```java
public record OrderCreatedEvent(String orderId, String email) implements Event {

    @Override
    public Class<?> type() {
        return OrderCreatedEvent.class;
    }

    @Override
    public List<Class<? extends EventChannel>> channels() {
        return List.of(InternalEventChannel.class, ExternalEventChannel.class);
    }
}

eventPublisher.publish(new OrderCreatedEvent("order-123", "user@example.com"));
```

**Use cases:** High-throughput scenarios, microservice-to-microservice communication, Kafka topics.

### 2. POJO/Record Publishing (Enveloped)

Publish any Java object directly — Envelope is created automatically:

```java
// Simple POJO with @Event annotation
@Event(channels = InternalEventChannel.class)
public record OrderCreated(String orderId, String email) {}

eventPublisher.publish(new OrderCreated("order-123", "user@example.com"));
```

Envelope with auto-generated metadata:
- `eventId` — random UUID
- `processId` — null
- `occurredAt` — current timestamp
- `metadata` — empty
- Channels — `InternalEventChannel` (default)

### 3. Full Control with `prepare()` Builder (Enveloped)

Use the builder for custom metadata and channels — same Envelope is created internally:

```java
eventPublisher.prepare(new OrderCreated("order-123", "user@example.com"))
    .withMetadata("key1", "data1")
    .withMetadata("key2", "data2")
    .withChannels(InternalEventChannel.class, ExternalEventChannel.class)
    .withProcessId(UUID.randomUUID())
    .withOccurredAt(Instant.now())
    .publish();
```

> **Note:** Channels specified via `withChannels()` have priority over channels defined in `@Event` annotation on the payload class.

**Available builder methods:**
- `withMetadata(key, value)` — add single metadata entry
- `withMetadata(Map)` — add multiple metadata entries
- `withChannel(channel)` — set single channel (convenience alias)
- `withChannels(c1)` — set one channel
- `withChannels(c1, c2)` — set two channels
- `withChannels(c1, c2, c3)` — set three channels
- `withChannels(List)` — set arbitrary number of channels
- `withProcessId(UUID)` — correlation ID (e.g., saga ID)
- `withOccurredAt(Instant)` — custom event timestamp
- `publish()` — send the event

### 4. POJO/Record with `@Event` Annotation

Specify default channels on the POJO/record class:

```java
@Event(channels = ExternalEventChannel.class)
public record OrderShipped(String orderId, Instant shippedAt) {}

@Event(channels = {InternalEventChannel.class, ExternalEventChannel.class})
public record OrderDelivered(String orderId, Instant deliveredAt) {}

eventPublisher.publish(new OrderShipped("order-123", Instant.now()));
```

### Comparison Table

| Approach | Best For | Envelope | Message Size |
|----------|----------|----------|--------------|
| `implements Event` | High throughput, Kafka, microservices | ❌ | Smallest |
| `publish(POJO/record)` | Simple notifications, internal events | ✅ | Medium |
| `publish(POJO/record) + @Event` | Default routing configuration | ✅ | Medium |
| `prepare().publish()` | Custom metadata, dynamic routing | ✅ | Medium |

### Event with Multiple Channels

```java
public record UserRegisteredEvent(String userId, String email) implements Event {

    @Override
    public Class<?> type() {
        return UserRegisteredEvent.class;
    }

    @Override
    public List<Class<? extends EventChannel>> channels() {
        return List.of(InternalEventChannel.class, ExternalEventChannel.class);
    }
}
```

### Interface-Based Handler

```java
public class NotificationEventSubscriber implements EventSubscriber {

    @Override
    public List<Class<? extends Event>> events() {
        return List.of(UserRegisteredEvent.class);
    }

    @Override
    public void onEvent(Event event) {
        if (event instanceof UserRegisteredEvent e) {
            sendWelcomeEmail(e.email());
        }
    }

    private void sendWelcomeEmail(String email) {
        // Email sending logic
    }
}

// Register the handler
handlerRegistry.register(new NotificationEventSubscriber());
```

### Handling Envelope (entire wrapper with metadata)

Handlers can receive the entire `Envelope` including metadata:

```java
public class OrderEventHandler {

    @EventListener
    public void handleOrder(Envelope<OrderPlacedEvent> envelope) {
        // Access payload
        OrderPlacedEvent event = envelope.payload();

        // Access metadata
        UUID eventId = envelope.eventId();
        UUID processId = envelope.processId();
        Instant occurredAt = envelope.occurredAt();
        Map<String, String> metadata = envelope.metadata();

        System.out.println("Processed: " + event.orderId());
    }
}
```

**Note:** When using `Envelope` as a handler parameter, you must specify the payload type in the annotation:

```java
@EventListener(OrderPlacedEvent.class)
public void handleOrder(Envelope<OrderPlacedEvent> envelope) {
    // Handle envelope
}
```

### Kafka Transport Configuration

```java
// Outgoing transport
OutTransport kafkaOut = new KafkaOutTransport(
    "localhost:9092",  // bootstrap servers
    "events"           // topic
);

EventChannel externalChannel = new ExternalEventChannel(
    List.of(kafkaOut)
);

// Incoming transport
InTransport kafkaIn = new KafkaInTransport(
    "localhost:9092",  // bootstrap servers
    "events",          // topics (comma-separated)
    "event-dispatcher" // group.id
);

// Dispatcher with Kafka transport
EventDispatcher dispatcher = EventDispatcherBuilder.create()
    .executor(Executors.newVirtualThreadPerTaskExecutor())
    .handlerRegistry(handlerRegistry)
    .transports(List.of(kafkaIn))
    .build();

dispatcher.start(dispatcher::dispatch);
```

### Idempotent Dispatcher

```java
EventDispatcher idempotentDispatcher = EventDispatcherBuilder.create()
    .executor(Executors.newVirtualThreadPerTaskExecutor())
    .handlerRegistry(handlerRegistry)
    .transports(List.of(inTransport))
    .idempotent(10000, Duration.ofMinutes(5))  // max 10000 entries, 5 min TTL
    .build();
```

### Transactional Publishing

For transactional publishing in Spring Boot applications, see [event-flow-spring/README.md](event-flow-spring/README.md).

## ⚙️ Configuration

For detailed configuration examples, see:
- **[Event Flow Core](event-flow-core/README.md)** — LocalQueue, Kafka, custom transports, serialization
- **[Event Flow Spring](event-flow-spring/README.md)** — YAML auto-configuration, transactional publishing, retry support, lifecycle tracking

### Lifecycle Tracking Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `event-flow.publisher.lifecycle.enabled` | `false` | Enable lifecycle-aware event publishing |
| `event-flow.publisher.lifecycle.service-name` | `""` | **Required!** Service name for ack filtering |
| `event-flow.publisher.lifecycle.store.type` | `db` | Store type: `db`, `in-memory`, or custom |
| `event-flow.publisher.lifecycle.store.table-name` | `event_store` | Custom table name (for `db` type) |
| `event-flow.publisher.lifecycle.store.auto-init-schema` | `true` | Auto-create table on startup |
| `event-flow.publisher.lifecycle.retry.enabled` | `true` | Enable automatic retry of failed events |
| `event-flow.publisher.lifecycle.retry.max-retries` | `3` | Maximum retry attempts |
| `event-flow.publisher.lifecycle.retry.retry-interval` | `30s` | Interval between retry cycles |
| `event-flow.publisher.lifecycle.retry.min-age` | `30s` | Base backoff for exponential retry |
| `event-flow.dispatcher.lifecycle.enabled` | `false` | Enable ack-based lifecycle tracking on dispatcher |

### LocalQueue

LocalQueue is a built-in in-JVM transport for internal event exchange. See [event-flow-core/README.md](event-flow-core/README.md#localqueue-in-jvm-transport) for details.

### Kafka

Kafka transport for external event communication. See [event-flow-core/README.md](event-flow-core/README.md#kafka-transport) for configuration examples.

### Custom Transport

Implement `OutTransport` or `InTransport` interfaces to add custom transport types. See [event-flow-core/README.md](event-flow-core/README.md#custom-transport) for an example.

## 🔊 Structured Logging

Event Flow provides structured JSON logging decorators for both the publisher and the dispatcher. Each log entry captures: event status, envelope metadata (eventId, processId, occurredAt), payload (truncated), handler/transport results, duration, and distributed tracing context (traceId, spanId, deliveredFrom).

### Enabling Logging

Use `buildAndLog()` or the `loggable()` builder methods:

```java
// Publisher
EventPublisher publisher = EventPublisherBuilder.create(channel)
    .loggable()                                    // defaults: 1024 char payload
    .loggable(500)                                 // custom payload truncation
    .loggable(500, Set.of("HeartbeatEvent"))       // exclude noisy events
    .loggable(500, Set.of(), Map.of("HeartbeatEvent", "ERROR"))  // with log overrides
    .build();

// Dispatcher
EventDispatcher dispatcher = EventDispatcherBuilder.create()
    .executor(executor)
    .handlerRegistry(registry)
    .loggable()
    .build();
```

### Per-Event Log Level Overrides

By default, log level is determined by the outcome:

| Outcome | Default level |
|---------|---------------|
| All handlers/transports succeed | `INFO` |
| Partial success (some fail) | `WARN` |
| All fail or exception | `ERROR` |

With `logLevels` you can override the minimum log level for specific event types. The override acts as a **threshold**:

| Override | ERROR outcome | WARN outcome | INFO outcome |
|----------|:------------:|:------------:|:------------:|
| `ERROR`  | `log.error`  | suppressed   | suppressed   |
| `WARN`   | `log.error`  | `log.warn`   | suppressed   |
| `INFO`   | `log.error`  | `log.warn`   | `log.info`   |

**Example:** Suppress logging for a high-frequency heartbeat event, only show errors:

```yaml
event-flow:
  dispatcher:
    logging:
      enabled: true
      log-levels:
        HeartbeatEvent: ERROR
        HealthCheckEvent: WARN
```

In Spring Boot, these settings go into `event-flow.yml` or `application.yml`. When using the builder directly (without Spring), pass the map via `loggable(maxPayloadLength, excludedEvents, logLevels)`:

```java
Map<String, String> logLevels = Map.of(
    "HeartbeatEvent", "ERROR",
    "HealthCheckEvent", "WARN"
);
EventDispatcher dispatcher = EventDispatcherBuilder.create()
    .executor(executor)
    .handlerRegistry(registry)
    .loggable(1024, Set.of(), logLevels)
    .build();
```

Valid level names: `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`.

## 📊 Interaction Diagrams

### 1. Event Publishing (Internal Channel)

```
┌─────────┐   ┌──────────────┐   ┌────────────┐  ┌───────────┐   ┌──────────┐  ┌───────────────┐
│ Service │   │EventPublisher│   │EventChannel│  │LocalQueue │   │Dispatcher│  │HandlerRegistry│
└────┬────┘   └──────┬───────┘   └─────┬──────┘  └─────┬─────┘   └────┬─────┘  └───────┬───────┘
     │               │                 │               │              │                │
     │ publish(event)│                 │               │              │                │
     │──────────────▶│                 │               │              │                │
     │               │ send(event)     │               │              │                │
     │               │────────────────▶│               │              │                │
     │               │                 │ offer(event)  │              │                │
     │               │                 │──────────────▶│              │                │
     │               │                 │               │              │                │
     │               │                 │               │ take(event)  │                │
     │               │                 │               │─────────────▶│                │
     │               │                 │               │              │                │
     │               │                 │               │              │getHandlers(event)
     │               │                 │               │              │───────────────▶│
     │               │                 │               │              │◀───────────────│
     │               │                 │               │              │ [handlers]     │
     │               │                 │               │              │                │
     │               │                 │               │              │ async execute  │
     │               │                 │               │              │───────────────▶│ onEvent(event)
     │               │                 │               │              │                │
     │◀──────────────│ (CompletableFuture)             │              │                │
```

### 2. Event Publishing (External Channel via Kafka)

```
┌─────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────┐        ┌──────────────┐
│ Service │   │EventPublisher│   │KafkaOutTransp│   │  Kafka   │        │KafkaInTransp │
└────┬────┘   └──────┬───────┘   └──────┬───────┘   └────┬─────┘        └──────┬───────┘
     │               │                  │                │                     │
     │ publish(event)│                  │                │                     │
     │──────────────▶│                  │                │                     │
     │               │ send(event)      │                │                     │
     │               │─────────────────▶│                │                     │
     │               │                  │ produce(event) │                     │
     │               │                  │───────────────▶│                     │
     │               │                  │                │                     │
     │               │                  │    ack(offset) │                     │
     │               │                  │◀───────────────│                     │
     │               │◀─────────────────│                │                     │
     │               │  SendResult      │                │                     │
     │◀──────────────│                  │                │                     │
     │               │                  │                │                     │
     │               │                  │                │ poll(event)         │
     │               │                  │                │────────────────────▶│
     │               │                  │                │                     │
     │               │                  │                │       event         │
     │               │                  │                │◀────────────────────│
```

### 3. Event Lifecycle Tracking (MANAGED)

```
┌──────────┐  ┌───────────────────┐  ┌──────────────┐  ┌────────────────┐  ┌──────────┐  ┌─────────┐
│ Service  │  │EventLifecycle     │  │  EventStore  │  │   EventChannel │  │Lifecycle │  │AckHandler│
│          │  │   Publisher       │  │  (persistent)│  │   + Dispatcher │  │Dispatcher│  │         │
└────┬─────┘  └────────┬──────────┘  └──────┬───────┘  └───────┬────────┘  └────┬─────┘  └────┬────┘
     │                 │                    │                  │                │             │
     │ publish(event)  │                    │                  │                │             │
     │────────────────▶│                    │                  │                │             │
     │                 │                    │                  │                │             │
     │                 │ save(NEW)          │                  │                │             │
     │                 │───────────────────▶│                  │                │             │
     │                 │                    │                  │                │             │
     │                 │ send(event)        │                  │                │             │
     │                 │──────────────────────────────────────▶│                │             │
     │                 │                    │                  │                │             │
     │                 │                    │                  │ dispatch(event)│             │
     │                 │                    │                  │───────────────▶│             │
     │                 │                    │                  │                │             │
     │                 │                    │                  │                │ handlers    │
     │                 │                    │                  │                │─────────▶   │
     │                 │                    │                  │                │◀────────    │
     │                 │                    │                  │                │ results     │
     │                 │                    │                  │                │             │
     │                 │ update(PUBLISHED)  │                  │                │             │
     │                 │◀───────────────────│                  │                │             │
     │                 │                    │                  │                │             │
     │                 │                    │                  │                │ publish ack │
     │                 │                    │                  │                │─────────────│──────────▶
     │                 │                    │                  │                │  (Success/  │           │
     │                 │                    │                  │                │   Failure)  │           │
     │                 │                    │                  │                │             │           │
     │                 │                    │                  │                │             │ ack event │
     │                 │                    │ update(HANDLED/  │                │             │◀──────────│
     │                 │                    │   FAILED)        │                │             │           │
     │                 │◀───────────────────│                  │                │             │           │
     │◀────────────────│ (CompletableFuture)│                  │                │             │           │
```

### 4. Event Dispatch to Multiple Handlers

```
┌──────────────┐   ┌───────────────┐   ┌─────────────────────────────────────┐
│  Dispatcher  │   │HandlerRegistry│   │          Event Handlers             │
└──────┬───────┘   └───────┬───────┘   └──────────────┬──────────────────────┘
       │                   │                          │
       │ dispatch(event)   │                          │
       │──────────────────▶│                          │
       │                   │                          │
       │                   │ getHandlers(event)       │
       │                   │─────────────────────────▶│
       │                   │                          │
       │                   │ [Handler1, Handler2, ...]│
       │                   │◀─────────────────────────│
       │                   │                          │
       │ handlers          │                          │
       │◀──────────────────│                          │
       │                   │                          │
       │ executor.execute(Handler1)                   │
       │─────────────────────────────────────────────▶│ Handler1.onEvent()
       │                   │                          │
       │ executor.execute(Handler2)                   │
       │─────────────────────────────────────────────▶│ Handler2.onEvent()
       │                   │                          │
       │       ...         │                          │
       │                   │                          │
```

## 📚 Documentation

- **[Event Flow Core](event-flow-core/README.md)** — Detailed core module documentation
- **[Event Flow Spring](event-flow-spring/README.md)** — Spring Boot auto-configuration
- [Javadoc](https://github.com/vovten/event-flow/javadoc)
- [Source Code](https://github.com/vovten/event-flow)
- [Usage Examples](https://github.com/vovten/event-flow/tree/main/src/test)

## 🤝 Contributing

1. Fork the repository
2. Create a branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the Apache License 2.0 — see the [LICENSE](LICENSE) file for details.

## 👥 Authors

- **Vladimir Aleshkov** ([@vovten](https://github.com/vovten))
