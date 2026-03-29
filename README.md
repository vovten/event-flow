# Event Flow

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Java](https://img.shields.io/badge/Java-21-blue)](https://openjdk.java.net/)
[![Kafka](https://img.shields.io/badge/Kafka-3.6.0-orange)](https://kafka.apache.org/)

**Event Flow** is a lightweight library for building event-driven architecture in Java applications. It provides a flexible and extensible system for publishing and processing events that works equally well in simple standalone applications and complex Spring-based projects.

## 📖 Table of Contents

- [Features](#-features)
- [Architecture](#-architecture)
- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Core Components](#-core-components)
- [Usage Examples](#-usage-examples)
- [Configuration](#-configuration)
- [Interaction Diagrams](#-interaction-diagrams)

## ✨ Features

- **Event Model** — Type-safe events with JSON serialization
- **Flexible Routing** — Event channels with configurable transports
- **Multiple Transports** — In-Memory and Apache Kafka support out of the box
- **Annotation-Based** — Event handling via `@EventListener`
- **Interface-Based** — Event handling via `EventListener` interface implementation
- **Spring Integration** — Automatic listener discovery in Spring Context
- **Transactional Publishing** — Send events after transaction commit
- **Retry Mechanism** — Exponential backoff retry attempts

## 🏗 Architecture

### High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Event Flow Architecture                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐         ┌─────────────────────────────────────────┐  │
│  │   Publisher  │────────▶│           Event Channels                │  │
│  │   (Builder)  │         │  ┌─────────────┐  ┌─────────────────┐   │  │
│  └──────────────┘         │  │   Internal  │  │    External     │   │  │
│                           │  │  (In-Memory)│  │    (Kafka)      │   │  │
│                           │  └──────┬──────┘  └────────┬────────┘   │  │
│                           │         │                 │                │
│                           └─────────┼─────────────────┼────────────┘   │
│                                     │                 │                │
│                           ┌─────────▼─────────────────▼────────────┐   │
│                           │     Outgoing Event Transports           │  │
│                           │  ┌─────────────┐  ┌─────────────────┐  │  │
│                           │  │  In-Memory  │  │      Kafka      │  │  │
│                           │  │   Queue     │  │    Producer     │  │  │
│                           │  └─────────────┘  └─────────────────┘  │  │
│                           └─────────────────────────────────────────┘  │
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
│                           ┌─────────────────────────────────────────┐  │
│                           │      Incoming Event Transports          │  │
│                           │  ┌─────────────┐  ┌─────────────────┐  │  │
│                           │  │  In-Memory  │  │      Kafka      │  │  │
│                           │  │   Queue     │  │    Consumer     │  │  │
│                           │  └─────────────┘  └─────────────────┘  │  │
│                           └─────────┬─────────────────┬────────────┘  │
│                                     │                 │               │
│                           ┌─────────▼─────────────────▼────────────┐  │
│                           │         Event Dispatcher                │  │
│                           │    (UnifiedEventDispatcher)             │  │
│                           └─────────┬─────────────────┬────────────┘  │
│                                     │                 │               │
│                           ┌─────────▼─────────────────▼────────────┐  │
│                           │        Listener Registry                │  │
│                           │  ┌─────────────┐  ┌─────────────────┐  │  │
│                           │  │ Annotation  │  │   Interface     │  │  │
│                           │  │   Based     │  │     Based       │  │  │
│                           │  └─────────────┘  └─────────────────┘  │  │
│                           └─────────┬─────────────────┬────────────┘  │
│                                     │                 │               │
│                           ┌─────────▼─────────────────▼────────────┐  │
│                           │          Event Listeners                │  │
│                           │    (@EventListener / Interface)         │  │
│                           └─────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Microservices Communication Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Microservice A (Publisher)                           │
│                                                                             │
│   ┌──────────┐    ┌───────────┐    ┌──────────┐    ┌──────────────────┐     │
│   │ Service  │───▶│ Publisher │───▶│ Channel  │───▶│ KafkaOutgoing  │     │
│   │          │    │           │    │          │    │ EventTransport   │     │
│   └──────────┘    └───────────┘    └──────────┘    └─────────┬────────┘     │
└───────────────────────────────────────────────────────────────┼─────────────┘
                                                                │
                                                                │  publish()
                                                                ▼
                    ═════════════════════════════════════════════
                    ═     Apache Kafka (Event Bus / Topic)      ═
                    ═════════════════════════════════════════════
                                                                │
                                                                │  consume()
                                                                │
┌───────────────────────────────────────────────────────────────┼─────────────┐
│                        Microservice B (Consumer)              │             │
│                                                               │             │
│   ┌──────────────────┐    ┌───────────┐    ┌──────────────┐   │             │
│   │ KafkaIncoming    │───▶│ Dispatcher│───▶│  Registry    │──┘             │
│   │ EventTransport   │    │           │    │              │                 │
│   └──────────────────┘    └─────┬─────┘    └──────┬───────┘                 │
│                                 │                  │                        │
│                                 │           ┌──────▼───────┐                │
│                                 │           │  Listener 1  │                │
│                                 │           └──────────────┘                │
│                                 │           ┌──────┐                        │
│                                 └──────────▶│ ...  │ (multiple listeners)   │
│                                             └──────┘                        │
│                                 ┌──────────▶┌──────┐                        │
│                                 │           │ ...  │                        │
│                                 │           └──────┘                        │
│                                 │           ┌──────────────┐                │
│                                 └──────────▶│ Listener N   │                │
│                                             └──────────────┘                │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Single Application Flow (Internal Events)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Single Application                                │
│                                                                             │
│   ┌──────────┐    ┌───────────┐    ┌──────────┐    ┌──────────────────┐     │
│   │ Service  │───▶│ Publisher │───▶│ Channel  │───▶│ LocalQueue       │     │
│   │          │    │           │    │          │    │ OutTransport     │     │
│   └──────────┘    └───────────┘    └──────────┘    └─────────┬────────┘     │
│                                                              │              │
│                                                              │ queue        │
│                                                              ▼              │
│   ┌──────────────────┐    ┌───────────┐    ┌──────────────┐                 │
│   │ LocalQueue       │◀───│ Dispatcher│◀───│  Registry    │                 │
│   │ InTransport      │    │           │    │              │                 │
│   └──────────────────┘    └─────┬─────┘    └──────┬───────┘                 │
│                                 │                  │                        │
│                                 │           ┌──────▼───────┐                │
│                                 └──────────▶│  Listeners   │                │
│                                             └──────────────┘                │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 📦 Installation

### Maven

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.github.vovten</groupId>
    <artifactId>event-flow</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Gradle

```groovy
implementation 'com.github.vovten:event-flow:1.0.0-SNAPSHOT'
```

### Requirements

- Java 21+
- Spring Boot 3.2.5+
- Apache Kafka 3.6.0+ (optional)

## 🚀 Quick Start

### 1. Configure Event Flow Infrastructure

First, set up the channels, transports, publisher, dispatcher, and listener registry:

```java
@Configuration
public class EventFlowConfig {

    @Bean
    public EventChannel internalChannel() {
        return new InternalEventChannel(
            List.of(new LocalQueueOutTransport(queueProvider.getQueue("internal")))
        );
    }

    @Bean
    public EventPublisher eventPublisher(List<EventChannel> channels) {
        return EventPublisherBuilder.channels(channels)
            .build();
    }

    @Bean
    public EventListenerRegistry listenerRegistry(ApplicationContext context) {
        return EventListenerRegistryBuilder.create()
            .withSpring(context, "com.example")
            .withAnnotationListeners()
            .build();
    }

    @Bean
    public EventDispatcher eventDispatcher(
            EventListenerRegistry registry,
            List<InTransport> transports) {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        return new UnifiedEventDispatcher(executor, registry, transports);
    }

    @Bean
    public InTransport incomingTransport() {
        return new LocalQueueInTransport(queueProvider.getQueue("internal"));
    }
}
```

### 2. Create an Event

```java
public record OrderCreatedEvent(String orderId, String customerId) implements Event {
    
    @Override
    public Class<? extends Event> type() {
        return OrderCreatedEvent.class;
    }
}
```

### 3. Create a Listener (Annotation-Based)

```java
@Component
public class OrderEventListener {

    @EventListener
    public void handleOrderCreated(OrderCreatedEvent event) {
        System.out.println("Order created: " + event.orderId());
    }
}
```

### 4. Publish an Event

```java
@Service
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
- 
### EventPublisher

Publishes events to configured channels.

**Creating via Builder:**

```java
EventPublisher publisher = EventPublisherBuilder.channels(internalChannel, externalChannel)
    .withRetry(3, Duration.ofMillis(100), 2.0)
    .transactional()
    .build();
```

### Builders

Event Flow provides fluent builders for convenient configuration of publishers and registries.

#### EventPublisherBuilder

Creates configured `EventPublisher` instances with flexible composition of features:

```java
// Simple publisher with channels only
EventPublisher publisher = EventPublisherBuilder.channels(channel1, channel2)
    .build();

// Publisher with retry and transaction support
EventPublisher publisher = EventPublisherBuilder.channels(channels)
    .withRetry(3, Duration.ofMillis(100), 2.0)
    .transactional()
    .build();

// Silent publisher with retry for analytics events
EventPublisher publisher = EventPublisherBuilder.channels(analyticsChannel)
    .withRetry()
    .silent()
    .build();

// Complete configuration with custom decorator
EventPublisher publisher = EventPublisherBuilder.channels(channels)
    .withRetry(5, Duration.ofSeconds(1), 1.5)
    .transactional()
    .withDecorator(pub -> new MetricsEventPublisher(pub, metricsRegistry))
    .silent()
    .build();
```

**Builder Options:**

| Method | Description |
|--------|-------------|
| `channels(...)` | Configure event channels (required) |
| `withRetry()` | Enable retry with default settings (3 attempts, 100ms initial delay, 2.0 multiplier) |
| `withRetry(max, delay, multiplier)` | Enable retry with custom settings |
| `transactional()` | Enable transactional publishing (defer until after commit) |
| `silent()` | Enable silent mode (catch and log all exceptions) |
| `withDecorator(fn)` | Add custom decorator to the publisher chain |
| `build()` | Build the publisher |
| `buildAndLog()` | Build and log the final configuration |

**Decorator Order** (from innermost to outermost):
1. Base `ChannelEventPublisher`
2. Custom decorators (in order added)
3. `RetryEventPublisher` (if enabled)
4. `TransactionalEventPublisher` (if enabled)
5. `SilentEventPublisher` (if enabled) — always outermost

#### EventListenerRegistryBuilder

Creates configured `EventListenerRegistry` instances with Spring integration support:

```java
// Simple annotation-based registry (non-Spring)
EventListenerRegistry registry = EventListenerRegistryBuilder.create()
    .withAnnotationListeners()
    .build();

// Spring-based registry with package scan
EventListenerRegistry registry = EventListenerRegistryBuilder.create()
    .withSpring(applicationContext, "com.example.listeners")
    .withAnnotationListeners()
    .withInterfaceListeners()
    .build();

// Composite registry with custom registries and decorators
EventListenerRegistry registry = EventListenerRegistryBuilder.create()
    .withAnnotationListeners()
    .withInterfaceListeners()
    .withCustomRegistry(customRegistry)
    .withDecorator(reg -> new LoggingEventListenerRegistry(reg))
    .build();
```

**Builder Options:**

| Method | Description |
|--------|-------------|
| `withSpring(context, package)` | Enable Spring integration with package scan (package is **required**) |
| `withAnnotationListeners()` | Discover listeners via `@EventListener` annotation |
| `withInterfaceListeners()` | Discover listeners implementing `EventListener` interface |
| `withCustomRegistry(registry)` | Add a custom registry to the composite |
| `withDecorator(fn)` | Add a decorator to wrap the registry |
| `build()` | Build the registry |
| `buildAndLog()` | Build and log the final configuration |
```

### EventDispatcher

Delivers events from transports to listeners.

```java
public interface EventDispatcher {
    void dispatch(Event event);
    void register(Object listener);
    boolean isRegistered(Object listener);
}
```

### EventListenerRegistry

Registry for discovering and managing listeners.

**Implementation Options:**
- `SpringAnnotationEventListenerRegistry` — scans Spring beans with `@EventListener`
- `SpringInterfaceEventListenerRegistry` — scans beans implementing `EventListener`
- `AnnotationEventListenerRegistry` — manual registration of annotated methods
- `InterfaceEventListenerRegistry` — manual registration of interface listeners
- `CompositeEventListenerRegistry` — combines multiple registries

### EventTransport

Transports for event delivery.

**Incoming Transports (`InTransport`):**
- `LocalQueueInTransport` — receive from in-memory queue
- `KafkaInTransport` — receive from Kafka topics

**Outgoing Transports (`OutTransport`):**
- `LocalQueueOutTransport` — send to in-memory queue
- `KafkaOutTransport` — send to Kafka topic
- `BroadcastKafkaOutTransport` — send to all Kafka topic partitions

## 📝 Usage Examples

### Creating an Event with Multiple Channels

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

### Interface-Based Listener

```java
@Component
public class NotificationEventListener implements EventListener {
    
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
```

### Configuring Channels and Transports

```java
@Configuration
public class EventFlowConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public EventChannel internalChannel() {
        return new InternalEventChannel(
            List.of(new LocalQueueOutTransport(queueProvider.getQueue("internal")))
        );
    }

    @Bean
    public EventChannel externalChannel() {
        return new ExternalEventChannel(
            List.of(new KafkaOutTransport(bootstrapServers, "events"))
        );
    }

    @Bean
    public EventPublisher eventPublisher(List<EventChannel> channels) {
        return EventPublisherBuilder.channels(channels)
            .withRetry(3, Duration.ofMillis(100), 2.0)
            .transactional()
            .buildAndLog();
    }

    @Bean
    public EventListenerRegistry listenerRegistry(ApplicationContext context) {
        return new CompositeEventListenerRegistry(List.of(
            new SpringAnnotationEventListenerRegistry(context, "com.example"),
            new SpringInterfaceEventListenerRegistry(context)
        ));
    }

    @Bean
    public EventDispatcher eventDispatcher(
            EventListenerRegistry registry,
            List<InTransport> transports) {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        return new UnifiedEventDispatcher(executor, registry, transports);
    }

    @Bean
    public InTransport kafkaIncomingTransport(
            @Value("${spring.kafka.bootstrap-servers}") String servers,
            @Value("${event.external.dispatcher.topics}") String topics,
            @Value("${event.external.dispatcher.group.id}") String groupId) {
        return new KafkaInTransport(servers, topics, groupId);
    }
}
```

### Publishing with Retry and Transactions

```java
@Service
public class PaymentService {

    private final EventPublisher eventPublisher;

    public PaymentService(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @Transactional
    public void processPayment(String orderId) {
        // Database transaction...
        
        // Event will be sent only after commit
        eventPublisher.publish(new PaymentCompletedEvent(orderId));
    }
}
```

### Silent Publishing for Non-Critical Events

```java
EventPublisher analyticsPublisher = EventPublisherBuilder.channels(analyticsChannel)
    .withRetry()
    .silent()  // Exceptions are logged but not propagated
    .build();

analyticsPublisher.publish(new PageViewEvent(userId, pageUrl));
```

## ⚙️ Configuration

### application.properties

```properties
# Kafka configuration
spring.kafka.bootstrap-servers=localhost:9092

# Event Flow configuration
event.dispatcher.thread.pool.size=10
event.listener.scan.package=com.example

# Unified dispatcher
event.dispatcher.enabled=true

# Internal event bus (in-memory)
event.internal.enabled=true

# External event bus (Kafka)
event.external.enabled=false
event.external.dispatcher.topics=eventflow.events
event.external.dispatcher.group.id=event-dispatcher

# Transactional publishing
event.transactional.publishing.enabled=true
```

## 📊 Interaction Diagrams

### 1. Event Publishing (Internal Channel)

```
┌─────────────┐      ┌──────────────────┐      ┌─────────────────┐      ┌──────────────┐
│   Service   │      │ EventPublisher   │      │ EventChannel    │      │  Dispatcher  │
└──────┬──────┘      └────────┬─────────┘      └────────┬────────┘      └──────┬───────┘
       │                      │                         │                      │
       │ publish(event)       │                         │                      │
       │─────────────────────▶│                         │                      │
       │                      │                         │                      │
       │                      │ send(event)             │                      │
       │                      │────────────────────────▶│                      │
       │                      │                         │                      │
       │                      │                         │ queue.offer(event)   │
       │                      │                         │─────────────────────▶│
       │                      │                         │                      │
       │                      │                         │                      │ poll(event)
       │                      │                         │                      │◀────────────┐
       │                      │                         │                      │             │
       │                      │                         │                      │ dispatch(e) │
       │                      │                         │                      │────────────▶│
       │                      │                         │                      │             │
       │                      │                         │                      │  getListeners()
       │                      │                         │                      │  ┌──────────┴──────┐
       │                      │                         │                      │  │ ListenerRegistry│
       │                      │                         │                      │  └──────────┬──────┘
       │                      │                         │                      │             │
       │                      │                         │                      │◀────────────┘
       │                      │                         │                      │
       │                      │                         │                      │ onEvent(event)
       │                      │                         │                      │────────────▶┌────────────┐
       │                      │                         │                      │             │  Listener  │
       │                      │                         │                      │             └────────────┘
```

### 2. Event Publishing (External Channel via Kafka)

```
┌─────────────┐      ┌──────────────────┐      ┌─────────────────┐      ┌──────────────┐
│   Service   │      │ EventPublisher   │      │  KafkaTransport │      │    Kafka     │
└──────┬──────┘      └────────┬─────────┘      └────────┬────────┘      └──────┬───────┘
       │                      │                         │                      │
       │ publish(event)       │                         │                      │
       │─────────────────────▶│                         │                      │
       │                      │                         │                      │
       │                      │ send(event)             │                      │
       │                      │────────────────────────▶│                      │
       │                      │                         │                      │
       │                      │                         │ producer.send()      │
       │                      │                         │─────────────────────▶│
       │                      │                         │                      │
       │                      │                         │                      │ offset
       │                      │                         │                      │◀─────────────┤
       │                      │                         │                      │
       │                      │                         │                      │
       │                      │                         │                      │ consume()
       │                      │                         │                      │◀────────────┐
       │                      │                         │                      │             │
       │                      │                         │                      │             │
       │                      │                         │                      │  deliver(e)  │
       │                      │                         │                      │─────────────▶│
       │                      │                         │                      │             │
       │                      │                         │                      │             │
```

### 3. Event Processing by Multiple Listeners

```
┌──────────────────┐      ┌───────────────────┐      ┌─────────────────────────────────┐
│   Dispatcher     │      │ ListenerRegistry  │      │         Event Listeners         │
└────────┬─────────┘      └─────────┬─────────┘      └─────────────────────────────────┘
         │                         │                                   │
         │ dispatch(event)         │                                   │
         │────────────────────────▶│                                   │
         │                         │                                   │
         │                         │ getListeners(event)               │
         │                         │──────────────────────────────────▶│
         │                         │                                   │
         │                         │◀──────────────────────────────────│
         │                         │ [Listener1, Listener2, Listener3] │
         │                         │                                   │
         │ listeners               │                                   │
         │◀────────────────────────│                                   │
         │                         │                                   │
         │ async execute           │                                   │
         │────────────────────────────────────────────────────────────▶│
         │                         │              ┌────────────────────┤
         │                         │              │ Listener1.onEvent()│
         │                         │              ├────────────────────┤
         │                         │              │ Listener2.onEvent()│
         │                         │              ├────────────────────┤
         │                         │              │ Listener3.onEvent()│
         │                         │              └────────────────────┘
```

### 4. Event Lifecycle in Microservices Architecture

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  Service A      │      │     Kafka       │      │  Service B      │      │  Service C      │
│  (Publisher)    │      │                 │      │  (Consumer)     │      │  (Consumer)     │
└────────┬────────┘      └────────┬────────┘      └────────┬────────┘      └────────┬────────┘
         │                        │                        │                        │
         │ publish(event)         │                        │                        │
         │───────────────────────▶│                        │                        │
         │                        │                        │                        │
         │                        │ produce(topic)         │                        │
         │                        │───────────────────────▶│                        │
         │                        │                        │                        │
         │                        │                        │ dispatch(event)        │
         │                        │                        │───────────────────────▶│
         │                        │                        │                        │
         │                        │                        │                        │ onEvent()
         │                        │                        │                        │───────────┐
         │                        │                        │                        │           │
         │                        │                        │                        │◀──────────┘
         │                        │                        │                        │
         │                        │ consume(topic)         │                        │
         │                        │◀───────────────────────│                        │
         │                        │                        │                        │
         │                        │                        │                        │ dispatch(event)
         │                        │                        │                        │───────────────────────▶│
         │                        │                        │                        │                        │
         │                        │                        │                        │                        │ onEvent()
         │                        │                        │                        │                        │───────────┐
         │                        │                        │                        │                        │           │
         │                        │                        │                        │                        │◀──────────┘
```

## 🔧 Extending

### Creating a Custom Transport

```java
public class RabbitMQOutTransport implements OutTransport {

    private final Channel channel;
    private final String exchange;

    @Override
    public String name() {
        return "rabbitmq";
    }

    @Override
    public void send(Event event) {
        // RabbitMQ sending logic
    }
}
```

### Creating a Custom Channel

```java
public class PriorityEventChannel implements EventChannel {

    private final List<OutTransport> transports;

    @Override
    public String name() {
        return "priority";
    }

    @Override
    public List<OutTransport> transports() {
        return transports;
    }
}
```

## 📚 Documentation

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

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

## 👥 Authors

- **Vladimir Aleshkov** ([@vovten](https://github.com/vovten))
