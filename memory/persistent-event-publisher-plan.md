# Persistent Event Publisher — план реализации

## 1. Status-модель

```
NEW ──publish──► PUBLISHED ──ack──► HANDLED
 │                                    │
 └──failed──► PUBLISH_FAILED ──retry──► NEW  (re-publish)
                                      │
                                      └──ack-failed──► HANDLE_FAILED ──retry──► NEW
```

### EventStatus (enum)

```java
NEW, PUBLISHED, PUBLISH_FAILED, HANDLED, HANDLE_FAILED
```

---

## 2. Core: пакеты и классы

### 2.1 Пакет `store/` — типы данных + EventStore

| Файл | Назначение |
|------|------------|
| `EventStatus.java` | enum с 5 статусами |
| `StoredEvent.java` | record — строка в EventStore |
| `EventStore.java` | интерфейс |
| `InMemoryEventStore.java` | ConcurrentHashMap-based, для тестов |
| `JdbcEventStore.java` | через `javax.sql.DataSource`, в core без Spring |

**StoredEvent:**
```java
public record StoredEvent(
    UUID eventId,
    String eventType,
    byte[] serializedPayload,
    String serializerCode,
    EventStatus status,
    int retryCount,
    Instant createdAt,
    Instant updatedAt,
    String errorDetails
) {}
```

**EventStore interface:**
```java
public interface EventStore {
    void save(StoredEvent event);
    void updateStatus(UUID eventId, EventStatus status, @Nullable String errorDetails);
    void incrementRetryCount(UUID eventId);
    List<StoredEvent> findByStatus(EventStatus status, Instant before);
    Optional<StoredEvent> findById(UUID eventId);
}
```

`updateStatus` автоматически проставляет `updatedAt = now()`.
`updateStatus` со статусом `NEW` также инкрементит `retryCount` (объединён по решению пользователя).

### 2.2 Пакет `publisher/` — PersistentEventPublisher

```java
public final class PersistentEventPublisher implements EventPublisher {
    private final EventPublisher origin;
    private final EventStore eventStore;
    private final EventSerializer serializer;

    public CompletableFuture<SendResults> publish(Event event) {
        // 1) Игнорируем LifecycleAckEvent → просто проксируем
        // 2) Ищем eventId в EventStore
        //    - если нет: сериализуем payload, сохраняем как NEW
        //    - если есть: updateStatus(NEW) (retry — ресет)
        // 3) origin.publish(event)
        // 4) on complete:
        //      success → updateStatus(PUBLISHED)
        //      failure → updateStatus(PUBLISH_FAILED, error)
    }
}
```

**Регистрация в `EventPublisherBuilder`:** новый метод `.persistent(EventStore, EventSerializer)`.

### 2.3 Пакет `lifecycle/` — ack-события и EventLifecycleDispatcher

**Ack-события:**
```java
public interface LifecycleAckEvent extends TraceableEvent {
    UUID originalEventId();
}

public record SuccessAck(
    UUID eventId, UUID originalEventId, String eventType,
    List<Class<? extends EventChannel>> sourceChannels,
    UUID processId, Instant occurredAt
) implements LifecycleAckEvent { ... }

public record FailureAck(
    UUID eventId, UUID originalEventId, String eventType, String error,
    List<Class<? extends EventChannel>> sourceChannels,
    UUID processId, Instant occurredAt
) implements LifecycleAckEvent { ... }
```

**EventLifecycleDispatcher** (декоратор `EventDispatcher`):

```java
public final class EventLifecycleDispatcher implements EventDispatcher {
    private final EventDispatcher origin;
    private final EventPublisher publisher;

    public CompletableFuture<HandlerResults> dispatch(Event event) {
        // Если event уже LifecycleAckEvent → просто origin.dispatch(event) (пропускаем, нет цикла)
        // Иначе:
        //   origin.dispatch(event)
        //   on complete:
        //     success → publisher.publish(SuccessAck(eventId, ..., event.channels()))
        //     failure → publisher.publish(FailureAck(eventId, error, ..., event.channels()))
    }
}
```

**AckHandler** — обработчик ack-событий, обновляет EventStore:

```java
// EventSubscriber для SuccessAck и FailureAck
// SuccessAck → eventStore.updateStatus(originalEventId, HANDLED)
// FailureAck → eventStore.updateStatus(originalEventId, HANDLE_FAILED, error)
```

### 2.4 Пакет `retry/` — EventRetryScheduler

```java
public final class EventRetryScheduler implements AutoCloseable {
    private final EventStore eventStore;
    private final EventPublisher publisher;  // PersistentEventPublisher (с сохранением)
    private final EventSerializer serializer;
    private final ScheduledExecutorService scheduler;
    private final Duration interval;
    private final Duration minAge;     // не трогать свежие записи
    private final int maxRetries;

    public void start() {
        scheduler.scheduleAtFixedRate(this::retry, 0, interval.toMillis(), MILLISECONDS);
    }

    void retry() {
        // 1) PUBLISH_FAILED, старше minAge, retryCount < maxRetries
        // 2) HANDLE_FAILED, старше minAge, retryCount < maxRetries
        // Для каждого: incrementRetryCount + updateStatus(NEW) + publisher.publish(deserializedEvent)
    }
}
```

---

## 3. Core: интеграция в билдеры

### EventPublisherBuilder

```java
public T persistent(EventStore eventStore, EventSerializer serializer) {
    this.persistentStore = eventStore;
    this.persistentSerializer = serializer;
    return (T) this;
}
```

В `build()` — после ChannelEventPublisher, перед decorate():

```java
if (persistentStore != null) {
    publisher = new PersistentEventPublisher(publisher, persistentStore, persistentSerializer);
}
```

### EventDispatcherBuilder.Chain

Lifecycle — ещё один элемент Chain:

```java
.chain()
    .add(d -> new IdempotentEventDispatcher(d, cache))
    .add(d -> new EventLifecycleDispatcher(d, ackPublisher))
    .add(LoggingEventDispatcher::new)
    .build();
```

---

## 4. Spring: autoconfig и properties

### EventFlowProperties — новое поле

```java
// Внутри PublisherConfig:
private PersistentPublisherConfig persistent = new PersistentPublisherConfig();

public static class PersistentPublisherConfig {
    private boolean enabled = false;
    private boolean retryEnabled = true;
    private int maxRetries = 3;
    private Duration retryInterval = Duration.ofSeconds(30);
    private Duration minAge = Duration.ofSeconds(10);
    private String serializer = "json";  // код сериализатора
}
```

```yaml
event-flow:
  publisher:
    persistent:
      enabled: true
      retry-enabled: true
      max-retries: 5
      retry-interval: 30s
      min-age: 10s
  dispatcher:
    lifecycle:
      enabled: true   # включает EventLifecycleDispatcher
```

### PersistentStoreConfiguration — новый autoconfig

```java
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow.publisher.persistent", name = "enabled", havingValue = "true")
public class PersistentStoreConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public EventStore eventStore(DataSource dataSource) {
        return new JdbcEventStore(dataSource);
    }

    @Bean
    public EventPublisher persistentEventPublisher(
            EventPublisher origin, EventStore store,
            EventSerializerFactory serializerFactory,
            EventFlowProperties properties) {
        EventSerializer serializer = serializerFactory.getSerializer(
            properties.getPublisher().getPersistent().getSerializer());
        return new PersistentEventPublisher(origin, store, serializer);
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnProperty(prefix = "event-flow.publisher.persistent", name = "retry-enabled",
                           havingValue = "true", matchIfMissing = true)
    public EventRetryScheduler retryScheduler(
            EventStore store, EventPublisher publisher,
            EventSerializerFactory serializerFactory,
            EventFlowProperties properties) {
        // ...
    }

    @Bean
    public AckHandler ackHandler(EventStore store, EventHandlerRegistry registry) {
        var handler = new AckHandler(store);
        registry.register(handler);
        return handler;
    }
}
```

### DispatcherConfiguration — доработка

Добавить опциональный `EventPublisher`:

```java
@Bean
@ConditionalOnProperty("event-flow.dispatcher.lifecycle.enabled")
public EventDispatcher eventDispatcher(
        ..., @Autowired(required=false) EventPublisher ackPublisher) {
    var builder = EventDispatcherBuilder.create()...;
    var dispatcher = builder.build();
    if (ackPublisher != null) {
        dispatcher = new EventLifecycleDispatcher(dispatcher, ackPublisher);
    }
    return dispatcher;
}
```

### EventFlowAutoConfiguration — импорт

```java
@Import({
    SerializerConfiguration.class,
    RegistryConfiguration.class,
    CommonConfiguration.class,
    ChannelConfiguration.class,
    PublisherConfiguration.class,
    PersistentStoreConfiguration.class,  // ← новый
    DispatcherConfiguration.class
})
```

---

## 5. Порядок реализации

| Шаг | Файлы | Что делаем |
|------|-------|------------|
| 1 | `EventStatus.java`, `StoredEvent.java` | Типы данных |
| 2 | `EventStore.java`, `InMemoryEventStore.java` | Интерфейс + тестовая реализация |
| 3 | `JdbcEventStore.java` | JDBC реализация с DDL |
| 4 | `EventPublisherBuilder.java` (доработка) | `.persistent()` метод |
| 5 | `PersistentEventPublisher.java` | Декоратор для EventPublisher |
| 6 | `SuccessAck.java`, `FailureAck.java`, `LifecycleAckEvent.java` | Ack-события |
| 7 | `EventLifecycleDispatcher.java` | Декоратор для Dispatcher |
| 8 | `AckHandler.java` | Обработчик ack-событий |
| 9 | `EventRetryScheduler.java` | Планировщик ретраев |
| 10 | `EventFlowProperties.java` (доработка) | PersistentPublisherConfig |
| 11 | `PersistentStoreConfiguration.java` | Spring autoconfig |
| 12 | `DispatcherConfiguration.java` (доработка) | Lifecycle tracking |
| 13 | `EventFlowAutoConfiguration.java` (доработка) | Импорт PersistentStoreConfiguration |
| 14 | Тесты | Unit + интеграционные |

---

## 6. DDL (JdbcEventStore)

```sql
CREATE TABLE IF NOT EXISTS event_store (
    event_id        UUID PRIMARY KEY,
    event_type      VARCHAR(512) NOT NULL,
    payload         BYTEA NOT NULL,
    serializer_code VARCHAR(8) NOT NULL,
    status          VARCHAR(32) NOT NULL,
    retry_count     INT DEFAULT 0 NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    error_details   TEXT
);

CREATE INDEX IF NOT EXISTS idx_event_store_status ON event_store(status, updated_at);
```

---

## 7. Границы и допущения

- `PersistentEventPublisher` **не сохраняет** `LifecycleAckEvent` (проверка `instanceof`)
- `EventLifecycleDispatcher` **пропускает** `LifecycleAckEvent` (не публикует ack на ack)
- Каналы для ack определяются из `event.channels()` оригинального события
- `EventStore` только на стороне **паблишера**; диспетчер ничего не пишет в БД
- `AckHandler` регистрируется в `EventHandlerRegistry` из `PersistentStoreConfiguration` (Spring)
- Для single-JVM всё работает через LocalQueue; для распределённого — через Kafka (KafkaInTransport / KafkaOutTransport + общий DataSource для EventStore)
- В будущем: идентификация диспетчера в multi-instance (через processId или отдельный instanceId)
