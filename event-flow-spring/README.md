# Event Flow Spring Boot Auto-Configuration

## Quick Start

### 1. Add Dependency

```xml
<dependency>
    <groupId>com.github.vovten</groupId>
    <artifactId>event-flow-spring</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 2. Configure (Required: scan-packages, channels, and transports)

```yaml
event-flow:
  scan-packages: com.example.listener
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

> **Note:** Both publisher and dispatcher are disabled by default. You must explicitly enable them and configure at least one channel/transport.

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

See [`event-flow-defaults.yml`](src/main/resources/event-flow-defaults.yml) for all available options with defaults and examples.

### Properties

| Property | Default | Description |
|----------|---------|-------------|
| `event-flow.enabled` | `true` | Enable/disable all auto-configuration |
| `event-flow.scan-packages` | `""` | **Required!** Packages to scan for listeners |
| `event-flow.publisher.enabled` | `false` | Enable/disable publisher |
| `event-flow.dispatcher.enabled` | `false` | Enable/disable dispatcher |
| `event-flow.publisher.transactional` | `true` | Defer publishing until after transaction commit |
| `event-flow.publisher.silent` | `false` | Catch and log all exceptions |
| `event-flow.publisher.retry.enabled` | `false` | Enable retry on publish failure |
| `event-flow.publisher.retry.max-attempts` | `3` | Maximum retry attempts |
| `event-flow.publisher.retry.initial-delay` | `100ms` | Initial delay between retries |
| `event-flow.publisher.retry.multiplier` | `2.0` | Exponential backoff multiplier |
| `event-flow.dispatcher.thread-pool.core-size` | `4` | Thread pool core size |
| `event-flow.dispatcher.thread-pool.max-size` | `16` | Thread pool max size |
| `event-flow.dispatcher.thread-pool.queue-capacity` | `100` | Event queue capacity |

---

## Usage Examples

### Minimal Configuration

```yaml
event-flow:
  scan-packages: com.example
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

> **Note:** You must explicitly enable publisher and dispatcher, and configure at least one channel/transport.

### Production with Kafka

```yaml
event-flow:
  scan-packages: com.example.listener
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
      - name: kafka-in
        topic: events
        servers: kafka:9092
        consumerGroup: my-service-group
```

### Custom Local-Queue Configuration

```yaml
event-flow:
  scan-packages: com.example
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
        type: kafka
        topic: events
        bootstrap-servers: localhost:9092
```

### Coupled Local-Queue Publisher/Dispatcher

By default, publisher and dispatcher share the same local-queue queue for efficient internal communication:

```yaml
event-flow:
  dispatcher:
    transports:
      - type: local-queue
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
| `SpringAnnotationEventListenerRegistry` | Scans @EventListener methods | Provide custom bean |
| `SpringInterfaceEventListenerRegistry` | Scans EventListener implementers | Provide custom bean |
| `EventListenerRegistry` | Composite of all registries | Provide custom bean |
| `eventFlowExecutor` | Thread pool for dispatcher | Provide `ExecutorService` bean |
| `localQueueProvider` | Shared queue for local-queue | Provide custom bean |
| `eventChannels` | All channels (internal + external) | Provide custom bean |
| `eventPublisher` | Main publisher with decorators | Provide `EventPublisher` bean |
| `eventDispatcher` | Main dispatcher | Provide `EventDispatcher` bean |
| `incomingEventTransports` | Additional incoming transports | Provide custom beans |
| `OutTransportFactory` / `InTransportFactory` implementations | Create transports from config | Provide custom factory |

### Channel Configuration

Channels are created from `event-flow.publisher.channels`:

```yaml
event-flow:
  publisher:
    channels:
      - name: internal      # → InternalEventChannel
        type: local-queue
        capacity: 1000
      - name: external      # → ExternalEventChannel
        type: kafka
        topic: events
      - name: custom        # → GenericEventChannel
        type: rabbitmq
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
        type: custom  # Your factory type
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

- [event-flow-defaults.yml](src/main/resources/event-flow-defaults.yml) - Complete configuration reference
- [Event Flow Core](../event-flow-core/README.md) - Core library documentation
