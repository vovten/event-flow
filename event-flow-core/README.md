# Event Flow Core

Detailed documentation for the core Event Flow library — framework-agnostic, pure Java 21+.

## 📖 Table of Contents

- [Quick Start](#-quick-start)
- [Events](#-events)
- [Channels & Transports](#-channels--transports)
- [Publisher](#-publisher)
- [Dispatcher](#-dispatcher)
- [Handler Registry](#-handler-registry)
- [Serialization](#-serialization)
- [Security](#-security)
- [Extension Points](#-extension-points)

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
var transports = LocalQueueTransportsBuilder.create("internal")
    .queueSize(1000)
    .build();

EventChannel channel = new InternalEventChannel(
    List.of(transports.outTransport())
);

EventPublisher publisher = EventPublisherBuilder.channels(channel)
    .withRetry(3, Duration.ofMillis(100), 2.0)
    .buildAndLog();

EventHandlerRegistry registry = EventHandlerRegistryBuilder.create()
    .withAnnotationListeners()
    .withInterfaceListeners()
    .buildAndLog();

EventDispatcher dispatcher = EventDispatcherBuilder.create()
    .executor(Executors.newVirtualThreadPerTaskExecutor())
    .handlerRegistry(registry)
    .transports(List.of(transports.inTransport()))
    .concurrencyLimit(100)
    .buildAndLog();

dispatcher.start();
```

### 3. Register a Handler & Publish

```java
public class OrderEventHandler {
    @EventListener
    public void handle(OrderCreatedEvent event) {
        System.out.println("Order created: " + event.orderId());
    }
}

registry.register(new OrderEventHandler());
publisher.publish(new OrderCreatedEvent("order-123", "customer-456"));
```

---

## 📝 Events

### Basic Event

Every event must implement the `Event` interface:

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

### TraceableEvent

For tracing and correlation, extend `TraceableEvent`:

```java
public record PaymentCompletedEvent(
    String paymentId,
    UUID uid,
    String traceId,
    Instant occurredAt
) implements TraceableEvent {

    @Override
    public Class<? extends Event> type() {
        return PaymentCompletedEvent.class;
    }

    @Override
    public UUID uid() { return uid; }

    @Override
    public String traceId() { return traceId; }

    @Override
    public Instant occurredAt() { return occurredAt; }
}
```

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

---

## 🔀 Channels & Transports

### EventChannel

A channel defines event delivery routes through transports:

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

### LocalQueue (In-JVM Transport)

```java
// Create a pair of transports sharing a BlockingDeque
var transports = LocalQueueTransportsBuilder.create("internal")
    .queueSize(1000)
    .build();

// Or with a custom queue
BlockingDeque<Event> customQueue = new LinkedBlockingDeque<>(500);
var transports = LocalQueueTransportsBuilder.create("internal")
    .queue(customQueue)
    .build();
```

### Kafka Transport

**Outgoing Transport:**

```java
// Basic setup
OutTransport kafkaOut = new KafkaOutTransport(
    "localhost:9092",  // bootstrap servers
    "events"           // topic
);

EventChannel externalChannel = new ExternalEventChannel(
    List.of(kafkaOut)
);

// Broadcast — send to all partitions
OutTransport broadcastKafkaOut = new BroadcastKafkaOutTransport(
    "localhost:9092",
    "events"
);
```

**Incoming Transport:**

```java
// Basic setup
InTransport kafkaIn = new KafkaInTransport(
    "localhost:9092",  // bootstrap servers
    "events",          // topics (comma-separated)
    "event-dispatcher" // group.id
);

// With custom Properties
Properties kafkaProps = new Properties();
kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
// ... other Kafka Consumer settings

InTransport kafkaIn = new KafkaInTransport(kafkaProps, "events", "event-dispatcher");
```

### Full Example: Internal + External

```java
var internalTransports = LocalQueueTransportsBuilder.create("internal")
    .queueSize(1000)
    .build();

OutTransport kafkaOut = new KafkaOutTransport("localhost:9092", "events");

EventChannel internalChannel = new InternalEventChannel(
    List.of(internalTransports.outTransport())
);
EventChannel externalChannel = new ExternalEventChannel(
    List.of(kafkaOut)
);

EventPublisher publisher = EventPublisherBuilder.channels(internalChannel, externalChannel)
    .build();

InTransport kafkaIn = new KafkaInTransport("localhost:9092", "events", "my-group");

EventDispatcher dispatcher = EventDispatcherBuilder.create()
    .executor(Executors.newVirtualThreadPerTaskExecutor())
    .handlerRegistry(registry)
    .transports(List.of(internalTransports.inTransport(), kafkaIn))
    .build();

dispatcher.start();
```

---

## 📤 Publisher

### EventPublisherBuilder

Fluent builder for creating publishers:

| Method | Description |
|--------|-------------|
| `channels(...)` | Configure event channels (required) |
| `withRetry()` | Enable retry with default settings (3 attempts, 100ms initial delay, 2.0 multiplier) |
| `withRetry(max, delay, multiplier)` | Enable retry with custom settings |
| `withDecorator(fn)` | Add custom decorator to the publisher chain |
| `build()` | Build the publisher |
| `buildAndLog()` | Build the publisher and log the configuration |

### Retry Configuration

```java
EventPublisher publisher = EventPublisherBuilder.channels(channel)
    .withRetry(
        5,                        // max attempts
        Duration.ofMillis(200),   // initial delay
        2.0                       // exponential backoff multiplier
    )
    .build();
```

---

## 📥 Dispatcher

### EventDispatcherBuilder

Fluent builder for creating dispatchers:

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

### Idempotent Dispatcher

```java
EventDispatcher idempotentDispatcher = EventDispatcherBuilder.create()
    .executor(Executors.newVirtualThreadPerTaskExecutor())
    .handlerRegistry(registry)
    .transports(List.of(inTransport))
    .idempotent(10000, Duration.ofMinutes(5))  // max 10000 entries, 5 min TTL
    .build();
```

---

## 📋 Handler Registry

### Annotation-Based Handler

```java
public class OrderEventHandler {

    @EventListener
    public void handleOrderCreated(OrderCreatedEvent event) {
        System.out.println("Order created: " + event.orderId());
    }

    @EventListener
    public void handleOrderCancelled(OrderCancelledEvent event) {
        System.out.println("Order cancelled: " + event.orderId());
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
```

### EventHandlerRegistryBuilder

| Method | Description |
|--------|-------------|
| `withAnnotationListeners()` | Enable discovery via `@EventListener` annotation |
| `withInterfaceListeners()` | Enable discovery via `EventSubscriber` interface |
| `withCustomRegistry(registry)` | Add a custom registry |
| `withDecorator(fn)` | Add a decorator |
| `build()` | Build the registry |
| `buildAndLog()` | Build the registry and log the configuration |

---

## 📦 Serialization

### EventSerializer

Serialization interface with magic byte prefix:
- `0x01` — JSON
- `0x02` — MessagePack

### Register Custom Serializer

```java
EventSerializerFactory.getInstance().register(new MyCustomSerializer());
```

### Custom Serializer Example

```java
public class ProtobufEventSerializer implements EventSerializer {

    @Override
    public byte getCode() {
        return 0x03;  // Unique code (0x01=json, 0x02=msgpack are reserved)
    }

    @Override
    public String getName() {
        return "protobuf";
    }

    @Override
    public byte[] serialize(Event event) {
        byte[] data = serializeToProtobuf(event);
        return Bytes.concat(new byte[]{getCode()}, data);
    }

    @Override
    public <T extends Event> T deserialize(byte[] data, Class<T> eventType) {
        return deserializeFromProtobuf(
            Arrays.copyOfRange(data, 1, data.length), eventType
        );
    }
}
```

---

## 🔒 Security

### EventTypeRegistry

Security whitelist for allowed event classes:

```java
// Allow a package (default: io.github.vovten.eventflow.*)
EventTypeRegistry.allowPackage("com.example.events");

// Allow a specific class
EventTypeRegistry.allowClass(MyEvent.class);
```

---

## 🔌 Extension Points

### Custom Transport

```java
public class RabbitMQOutTransport implements OutTransport {

    private final Channel channel;
    private final String exchange;

    public RabbitMQOutTransport(Channel channel, String exchange) {
        this.channel = channel;
        this.exchange = exchange;
    }

    @Override
    public String name() {
        return "rabbitmq";
    }

    @Override
    public CompletableFuture<SendResult> send(Event event) {
        // RabbitMQ sending logic
        return CompletableFuture.completedFuture(
            SendResult.success("rabbitmq", exchange)
        );
    }
}
```

### Custom Channel

```java
public class RabbitMQEventChannel implements EventChannel {

    private final List<OutTransport> transports;

    public RabbitMQEventChannel(OutTransport transport) {
        this.transports = List.of(transport);
    }

    @Override
    public String name() {
        return "rabbitmq";
    }

    @Override
    public List<OutTransport> transports() {
        return transports;
    }

    @Override
    public void send(Event event) {
        transports.forEach(t -> t.send(event));
    }
}
```

---

## 📚 See Also

- [Event Flow Spring](../event-flow-spring/README.md) — Spring Boot auto-configuration with YAML
- [Main README](../README.md) — Project overview and architecture
