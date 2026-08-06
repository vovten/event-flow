# Event Flow Spring

**Event Flow** with Spring Framework support. This module provides **automatic configuration** via YAML, eliminating boilerplate setup code.

**What it gives you:**
- Zero-config `EventPublisher` and `EventDispatcher` beans
- YAML-based transport and channel configuration
- Transactional publishing (events sent only after DB commit)
- Automatic listener discovery (`@EventListener` scanning)
- Pluggable transport factories (LocalQueue, Kafka, custom)

## Quick Start

### 1. Add Dependency

```xml
<dependency>
    <groupId>io.github.vovten</groupId>
    <artifactId>event-flow-spring</artifactId>
    <version>1.2.3</version>
</dependency>
```

### 2. Configure (Required: enable and configure components)

```yaml
event-flow:
  enabled: true
  dispatcher.listener-packages: com.example.listener
  publisher:
    enabled: true
    channels:
      - name: internal
        transports:
          - name: local-queue
            capacity: 1000
    transactional: true
    retry:
      enabled: true
      max-attempts: 3
  dispatcher:
    enabled: true
    transports:
      - name: local-queue
        capacity: 1000
```

> **Note:** All components are disabled by default. You must explicitly enable `event-flow`, `publisher`, and `dispatcher` in your configuration.

### 3. Create Event Listeners

```java
@Component
public class OrderEventListener {

    @EventListener
    public void handle(OrderCreatedEvent event) {
        // Handle event
    }
}
```

### 4. Publish Events

```java
@Autowired
private EventPublisher eventPublisher;

public void createOrder() {
    eventPublisher.publish(new OrderCreatedEvent(...));
}
```

---

## Configuration Reference

### Default Configuration File

See [`event-flow.yml`](src/main/resources/event-flow.yml) for all available options with defaults and examples.

### Properties

| Property | Default | Description |
|----------|---------|-------------|
| `event-flow.enabled` | `false` | Enable/disable all auto-configuration |
| `event-flow.dispatcher.listener-packages` | `""` | **Required!** Packages to scan for listeners |
| `event-flow.publisher.enabled` | `false` | Enable/disable publisher |
| `event-flow.dispatcher.enabled` | `false` | Enable/disable dispatcher |
| `event-flow.publisher.transactional` | `true` | Defer publishing until after transaction commit |
| `event-flow.publisher.retry.enabled` | `false` | Enable retry on publish failure |
| `event-flow.publisher.retry.max-attempts` | `3` | Maximum retry attempts |
| `event-flow.publisher.retry.initial-delay` | `100ms` | Initial delay between retries |
| `event-flow.publisher.retry.multiplier` | `2.0` | Exponential backoff multiplier |
| `event-flow.dispatcher.thread-pool.core-size` | `4` | Thread pool core size (ignored with virtual threads) |
| `event-flow.dispatcher.thread-pool.max-size` | `16` | Thread pool max size (ignored with virtual threads) |
| `event-flow.dispatcher.thread-pool.queue-capacity` | `100` | Event queue capacity (ignored with virtual threads) |
| `event-flow.dispatcher.thread-pool.keep-alive-seconds` | `60` | Keep-alive time for idle threads (ignored with virtual threads) |
| `event-flow.dispatcher.thread-pool.concurrency-limit` | `0` | Max concurrent handler executions (0 = no limit, backpressure for virtual threads) |
| `event-flow.dispatcher.deserialization.allowed-event-packages` | `["io.github.vovten.eventflow"]` | Allowed packages for secure deserialization |
| `event-flow.dispatcher.idempotent.enabled` | `false` | Enable idempotent event processing (deduplication by UID) |
| `event-flow.dispatcher.idempotent.ttl` | `10m` | Time-to-live for cached event UIDs |
| `event-flow.dispatcher.idempotent.max-size` | `10000` | Maximum entries in the idempotency cache |
| `event-flow.dispatcher.idempotent.warn-on-duplicate` | `true` | Log warnings when duplicate events are received |

---

## Usage Examples

### Minimal Configuration

```yaml
event-flow:
  enabled: true
  dispatcher.listener-packages: com.example
  publisher:
    enabled: true
    channels:
      - name: internal
        transports:
          - name: local-queue
            capacity: 1000
  dispatcher:
    enabled: true
    transports:
      - name: local-queue
        capacity: 1000
```

> **Note:** You must explicitly enable `event-flow`, `publisher`, and `dispatcher`, and configure at least one channel/transport.

### Production with Kafka

```yaml
event-flow:
  dispatcher.listener-packages: com.example.listener
  publisher:
    enabled: true
    transactional: true
    retry:
      enabled: true
      max-attempts: 3
      initial-delay: 200ms
    channels:
      - name: internal
        transports:
          - name: local-queue
            capacity: 1000
      - name: external
        transports:
          - name: kafka
            topic: events
            servers: kafka:9092
  dispatcher:
    enabled: true
    thread-pool:
      core-size: 8
      max-size: 32
      queue-capacity: 500
    transports:
      - name: local-queue
        capacity: 1000
      - name: kafka
        topic: events
        servers: kafka:9092
        consumerGroup: my-service-group
```

### Custom Local-Queue Configuration

```yaml
event-flow:
  dispatcher.listener-packages: com.example
  publisher:
    enabled: true
    channels:
      - name: internal
        transports:
          - name: local-queue
            capacity: 500
  dispatcher:
    enabled: true
    thread-pool:
      core-size: 2
      max-size: 4
      queue-capacity: 500
    transports:
      - name: local-queue
        capacity: 500
```

### Disable Auto-Configuration

```yaml
event-flow:
  enabled: false
```

Then provide custom beans:

```java
@Configuration
public class CustomEventFlowConfig {
    
    @Bean
    public EventPublisher eventPublisher() {
        // Custom configuration
    }
    
    @Bean
    public EventDispatcher eventDispatcher() {
        // Custom configuration
    }
}
```

### Custom Transport Factory

Implement `OutTransportFactory` or `InTransportFactory` to add custom transport types:

```java
@Component
public class RabbitMqTransportFactory implements OutTransportFactory, InTransportFactory {

    @Override
    public String getName() {
        return "rabbitmq";
    }

    @Override
    public OutTransport createPublisher(EventFlowProperties.ChannelConfig config) {
        return new RabbitMqOutTransport(config.getRabbitMqConfig());
    }

    @Override
    public InTransport createDispatcher(EventFlowProperties.TransportConfig config) {
        return new RabbitMqInTransport(config.getRabbitMqConfig());
    }
}
```

### Custom Event Serializers

Implement `EventSerializer` and annotate with `@Component` to auto-register it in `EventSerializerFactory`:

```java
@Component
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
        // Serialize event using Protocol Buffers
        // First byte must be the code (0x03)
        byte[] data = serializeToProtobuf(event);
        return Bytes.concat(new byte[]{getCode()}, data);
    }

    @Override
    public <T extends Event> T deserialize(byte[] data, Class<T> eventType) {
        // Skip first byte (code) and deserialize
        return deserializeFromProtobuf(Arrays.copyOfRange(data, 1, data.length), eventType);
    }
}
```

The serializer will be automatically discovered and registered when `event-flow.enabled=true`.

---

## Features

### Transactional Publishing

Events are published only after the current transaction commits:

```java
@Transactional
public void processOrder() {
    orderRepository.save(order);  // DB operation
    eventPublisher.publish(new OrderCreatedEvent(order));  // Published after commit
}
```

### Retry Support

Automatic retry with exponential backoff:

```yaml
event-flow:
  publisher:
    retry:
      enabled: true
      max-attempts: 3
      initial-delay: 100ms
      multiplier: 2.0
```

### Multiple Channels

Route events through different transports:

```yaml
event-flow:
  publisher:
    channels:
      - name: external
        transports:
          - name: kafka
            topic: events
            servers: localhost:9092
```

### Coupled Local-Queue Publisher/Dispatcher

By default, publisher and dispatcher share the same local-queue queue for efficient internal communication:

```yaml
event-flow:
  dispatcher:
    transports:
      - name: local-queue
        capacity: 1000  # Shared queue size
```

---

## Architecture

### Transport Factory Pattern

The auto-configuration uses pluggable `OutTransportFactory` and `InTransportFactory` SPIs:

```
OutTransportFactory (interface)
├── LocalQueueOutTransportFactory (default)
└── KafkaOutTransportFactory (auto-discovered)

InTransportFactory (interface)
├── LocalQueueInTransportFactory (default)
└── KafkaInTransportFactory (auto-discovered)
```

New transport types can be added by implementing `OutTransportFactory` or `InTransportFactory` and annotating with `@Component`.

### Auto-Configured Beans

| Bean | Description | Override |
|------|-------------|----------|
| `springEventListenerRegistry` | Scans @EventListener methods in configured packages | Provide custom bean |
| `springEventSubscriberRegistry` | Scans EventSubscriber implementers | Provide custom bean |
| `eventHandlerRegistry` | Composite of all registries | Provide custom bean |
| `dispatcherExecutor` | Thread pool for dispatcher (virtual threads) | Provide `ExecutorService` bean |
| `localQueueProvider` | Shared queue for local-queue | Provide custom bean |
| `eventChannels` | All channels (internal + external) | Provide custom bean |
| `eventPublisher` | Main publisher with decorators | Provide `EventPublisher` bean |
| `eventDispatcher` | Main dispatcher | Provide `EventDispatcher` bean |
| `dispatcherTransports` | Additional incoming transports | Provide custom beans |
| `OutTransportFactory` / `InTransportFactory` implementations | Create transports from config | Provide custom factory |
| `EventSerializer` beans | Custom serializers auto-registered in `EventSerializerFactory` | Implement `EventSerializer` + `@Component` |

### Channel Configuration

Channels are created from `event-flow.publisher.channels`:

```yaml
event-flow:
  publisher:
    channels:
      - name: internal      # → InternalEventChannel
        transports:
          - name: local-queue
            capacity: 1000
      - name: external      # → ExternalEventChannel
        transports:
          - name: kafka
            topic: events
            servers: kafka:9092
      - name: custom        # → GenericEventChannel
        transports:
          - name: rabbitmq
            ...
```

---

## Extension Points

### Custom Transport Factory

1. Implement `OutTransportFactory` or `InTransportFactory`:

```java
@Component
public class CustomTransportFactory implements OutTransportFactory, InTransportFactory {

    @Override
    public String getName() {
        return "custom";
    }

    @Override
    public OutTransport createPublisher(EventFlowProperties.ChannelConfig config) {
        // Create outgoing transport
    }

    @Override
    public InTransport createDispatcher(EventFlowProperties.TransportConfig config) {
        // Create incoming transport
    }

    @Override
    public void validate(EventFlowProperties.ChannelConfig config) {
        // Custom validation
    }
}
```

2. Use in configuration:

```yaml
event-flow:
  publisher:
    channels:
      - name: my-channel
        transports:
          - name: custom  # Your factory type
            # Custom properties...
```

### Custom Channel Implementation

Provide your own `EventChannel` bean:

```java
@Bean
public EventChannel customChannel() {
    return new CustomEventChannel();
}
```

---

## See Also

- [event-flow.yml](src/main/resources/event-flow.yml) - Complete configuration reference
- [Event Flow Core](../event-flow-core/README.md) - Core module documentation
