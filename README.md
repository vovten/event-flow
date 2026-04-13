# Event Flow

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java](https://img.shields.io/badge/Java-21-blue)](https://openjdk.java.net/)

**Event Flow** is a lightweight Java library for building event-driven architectures. It provides a flexible event publishing and processing system that works equally well in simple standalone applications and complex projects using DI frameworks.

## 📖 Table of Contents

- [Features](#-features)
- [Architecture](#-architecture)
- [Modules](#-modules)
- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Core Components](#-core-components)
- [Usage Examples](#-usage-examples)
- [Configuration](#-configuration)
- [Interaction Diagrams](#-interaction-diagrams)

## ✨ Features

- **Typed Events** — Events with JSON serialization and polymorphic deserialization
- **Flexible Routing** — Event channels with configurable transports
- **Multiple Transports** — LocalQueue (in-JVM) and Apache Kafka out of the box
- **Annotation-Based** — Event handling via `@EventListener`
- **Interface-Based** — Event handling via `EventSubscriber` interface
- **Idempotency** — Event deduplication based on UID
- **Transactional Publishing** — Send events after transaction commit
- **Retry Mechanism** — Exponential backoff with configurable parameters
- **Extensible Serialization** — JSON and MessagePack with support for custom formats
- **Security** — Event class whitelist to protect against deserialization attacks

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
| **event-flow-core** | Core library — framework-agnostic, pure Java 21+ | [README](event-flow-core/README.md) |
| **event-flow-spring** | Spring Boot auto-configuration with YAML | [README](event-flow-spring/README.md) |

## 📦 Installation

### Maven

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.vovten</groupId>
    <artifactId>event-flow</artifactId>
    <version>1.0.0</version>
</dependency>
```

For Spring Boot integration:

```xml
<dependency>
    <groupId>io.github.vovten</groupId>
    <artifactId>event-flow-spring</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'io.github.vovten:event-flow:1.0.0'
// For Spring Boot:
implementation 'io.github.vovten:event-flow-spring:1.0.0'
```

### Requirements

- Java 21+
- Apache Kafka 3.6.0+ (optional, for external events)

## 🚀 Quick Start

### 1. Create an Event

```java
public record OrderCreatedEvent(String orderId, String customerId) implements Event {

    @Override
    public Class<? extends Event> type() {
        return OrderCreatedEvent.class;
    }
}
```

### 2. Set Up Infrastructure

```java
// Create transports for internal and external communication
var internalTransports = new LocalQueueTransportsBuilder()
    .queueSize(1000)
    .build();

var externalTransports = new LocalQueueTransportsBuilder()
    .queueSize(1000)
    .build();

// Create channels
EventChannel internalChannel = new InternalEventChannel(
    List.of(internalTransports.outTransport())
);
EventChannel externalChannel = new ExternalEventChannel(
    List.of(externalTransports.outTransport())
);

// Create publisher
EventPublisher eventPublisher = EventPublisherBuilder.create(internalChannel, externalChannel)
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
    .transports(List.of(internalTransports.inTransport(), externalTransports.inTransport()))
    .concurrencyLimit(100)
    .buildAndLog();

// Start the dispatcher
eventDispatcher.start();
```

### 3. Create a Handler (Annotation-Based)

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
    Class<? extends Event> type();
    default List<Class<? extends EventChannel>> channels() {
        return List.of(InternalEventChannel.class);
    }
    default String asJson() {
        return EventUtils.toJson(this);
    }
}
```

**TraceableEvent** — extends `Event` with tracing fields: `uid` (UUID), `traceId` (correlation), `occurredAt` (timestamp).

### EventChannel

A channel defines event delivery routes through transports.

```java
public interface EventChannel {
    String name();
    List<OutTransport> transports();
    void send(Event event);
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
    List.of(transports.outTransport())
);

EventDispatcher dispatcher = EventDispatcherBuilder.create()
    .executor(Executors.newVirtualThreadPerTaskExecutor())
    .handlerRegistry(registry)
    .transports(List.of(transports.inTransport()))
    .build();
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

## 📝 Usage Examples

### Event with Multiple Channels

```java
public record UserRegisteredEvent(String userId, String email) implements Event {

    @Override
    public Class<? extends Event> type() {
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

dispatcher.start();
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
- **[Event Flow Spring](event-flow-spring/README.md)** — YAML auto-configuration, transactional publishing, retry support

### LocalQueue

LocalQueue is a built-in in-JVM transport for internal event exchange. See [event-flow-core/README.md](event-flow-core/README.md#localqueue-in-jvm-transport) for details.

### Kafka

Kafka transport for external event communication. See [event-flow-core/README.md](event-flow-core/README.md#kafka-transport) for configuration examples.

### Custom Transport

Implement `OutTransport` or `InTransport` interfaces to add custom transport types. See [event-flow-core/README.md](event-flow-core/README.md#custom-transport) for an example.

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

### 3. Event Dispatch to Multiple Handlers

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

- **[Event Flow Core](event-flow-core/README.md)** — Detailed core library documentation
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
