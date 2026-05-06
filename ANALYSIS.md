# Event Flow — Экспертный анализ

> Анализ выполнен с позиции архитектора ПО / эксперта в области программирования.

---

## 1. Архитектура и дизайн

### Оценка: **7/10**

**Сильные стороны:**

- Разделение `event-flow-core` (чистый Java 21+) и `event-flow-spring` выполнено грамотно. Spring-зависимости помечены `optional` — это корректный подход для библиотеки. Core можно использовать без Spring-контекста.
- Decorator chain для publisher (`ChannelEventPublisher → RetryEventPublisher → TransactionalEventPublisher`) и dispatcher (`UnifiedEventDispatcher → IdempotentEventDispatcher`) — правильное применение паттерна. Цепочки расширяются без изменения ядра.
- CRTP Builders (`EventPublisherBuilder<T>`, `EventHandlerRegistryBuilder<T>`) обеспечивают типобезопасную fluent-цепочку в наследниках — нетривиальный и правильный выбор для библиотечного API.
- `SendResult` / `SendResults` как value objects с богатым query API — хороший API-дизайн.
- `BroadcastEventChannel` + `BroadcastKafkaOutTransport` — корректное разделение: канал декларирует намерение, транспорт реализует механику.

**Проблемы:**

**Критическая архитектурная ошибка:** `Event.channels()` возвращает `List<Class<? extends EventChannel>>`. Это означает, что доменное событие знает о инфраструктурных каналах. Нарушение DDD и Dependency Inversion. `OrderCreatedEvent` не должен ничего знать об `ExternalEventChannel`.

```java
// Так — плохо: доменная модель зависит от инфраструктуры
public List<Class<? extends EventChannel>> channels() {
    return List.of(InternalEventChannel.class, ExternalEventChannel.class);
}
```

Правильно: routing должен быть вынесен в конфигурацию (маппинг `EventType → Channel[]`) или в аннотацию `@RouteTo("external")` на классе события — без импорта инфраструктурных типов.

- `AbstractEventDispatcher` — мёртвый код, не используется `UnifiedEventDispatcher`, но занимает место в API.
- `ChannelConfiguration` и `PublisherConfiguration` дублируют логику сборки `Map<String, OutTransportFactory>`.

---

## 2. Качество кода

### Оценка: **7.5/10**

**Сильные стороны:**

- Javadoc на публичном API присутствует и содержателен (не просто геттер-описания).
- Checkstyle с реальными правилами (не дефолт Google Style): запрет пустых блоков, порядок деклараций, `InnerTypeLast`, `MethodLength ≤ 100` строк — культура кода ощущается.
- `AtomicBoolean` с `compareAndSet` для защиты от двойного старта транспортов — аккуратная работа с состоянием.
- Immutable records (`SendResult`, `EventHandler`) там, где это уместно.
- Тесты именуются через `@DisplayName` + snake_case методы — читаемо.

**Проблемы:**

- `EventTypeRegistry` — глобальный статический мутируемый state (`static ConcurrentHashSet`). Это антипаттерн для библиотеки: ломает параллельное тестирование, несовместим с multi-tenant / multi-context сценариями. Должен быть экземпляром, переданным через DI.
- `EventUtils` дублирует конфигурацию `ObjectMapper` из `JsonEventSerializer`. При любом изменении надо помнить про оба места.
- `PublisherConfiguration` возвращает `null` из `@Bean`-метода при отсутствии каналов. `@Bean` метод не должен возвращать `null` — это тихая бомба.
- `KafkaInTransportFactory.createDispatcher()` игнорирует инжектированный `serializerFactory` — bug, а не smell. Кастомные сериализаторы не работают для входящих Kafka-событий.

---

## 3. Новизна подхода и сравнение с аналогами

### Оценка: **4/10**

Это честная оценка — не уничижительная, а реалистичная.

**Что делает то же самое, только лучше:**

| Аналог | Что умеет лучше |
|---|---|
| **Spring ApplicationEventPublisher** | Встроен в Spring, нет overhead на конфигурацию, `@TransactionalEventListener` из коробки, async через `@Async` |
| **Spring Cloud Stream** | Полноценная абстракция над Kafka/RabbitMQ/Pulsar, binding-модель, retry/DLQ из коробки, cloud-native |
| **Axon Framework** | Event sourcing + CQRS, saga-паттерн, реплика состояния, tracking processors с позициями |
| **MicroProfile Reactive Messaging** | Стандарт Jakarta EE, Quarkus/Wildfly, connector-модель |
| **Guava EventBus** | Для in-JVM, аннотация `@Subscribe`, простота |
| **Reactor / RxJava** | Реактивный pipeline с backpressure первого класса, операторы, non-blocking от начала до конца |
| **Apache Camel** | Enterprise Integration Patterns (EIP) в полном объёме |

**Что Event Flow делает интересно, но не уникально:**

- Magic byte framing для auto-detection формата сериализации — используется в Confluent Schema Registry и Avro-based системах
- `EventPolymorphicTypeValidator` как защита от Jackson gadget-атак — правильно, но это просто решение известной проблемы Jackson, описанной в CVE-2017-7525 и последующих
- CRTP fluent builders — встречается в проектах класса AssertJ, AWS SDK v2
- Decorator chain для publisher/dispatcher — стандартный паттерн

**Объективный вывод:** Event Flow — это хорошо структурированная, аккуратная библиотека-обёртка. Она решает реальную задачу: дать простой, Spring-совместимый event bus с Kafka без boilerplate. Но она не предлагает ни новой абстракции, ни нового решения известных проблем. Ниша узкая: тем, кому Spring Events недостаточно, но Spring Cloud Stream избыточен.

---

## 4. Производительность

### Оценка: **6.5/10**

### Путь события через систему

```
publish() → channel.send() → transport.send() → [queue/kafka] → poll() → dispatch() → handler.onEvent()
```

**In-JVM (LocalQueue) путь:**

- `LocalQueueOutTransport.offer()` — O(1), неблокирующий. Хорошо.
- `LocalQueueInTransport` — один virtual thread, `queue.take()` (blocking). Событие доставляется к dispatcher'у последовательно. Параллельно событий из очереди не читается — **это bottleneck при высоком throughput**.
- `UnifiedEventDispatcher.dispatch()` — для каждого handler'а: `executorService.execute()`. При виртуальных потоках — дёшево. При `Semaphore` — блокирует транспортный поток при исчерпании permit'ов.
- `EventListenerRegistry.getHandlers()` — `ConcurrentHashMap.get()` + `computeIfAbsent` для cache. Практически O(1) на hot path.
- `RetryEventPublisher` — использует `CompletableFuture.delayedExecutor()` → common ForkJoinPool. При высокой частоте retry'ев давит на общий пул.

**Kafka путь:**

- `KafkaOutTransport` — async producer с callback-мостом в `CompletableFuture`. Конфигурация правильная: `acks=all`, idempotent, 32MB buffer.
- `KafkaInTransport` — один platform thread, poll с 100ms timeout. **Жёстко однопоточный consumer**. Один consumer = один partition (в рамках group) активен одновременно. Для высокого throughput нужно несколько KafkaInTransport-инстансов с разными `consumerGroup` или партициями.
- `BroadcastKafkaOutTransport.send()` вызывает `producer.partitionsFor(topic)` **при каждой отправке**. Это синхронный сетевой вызов (или кэш в клиенте, но не гарантировано). При 10 партициях создаёт 10 individual ProducerRecord и 10 CompletableFuture.

**Оценочные цифры throughput:**

| Сценарий | Ожидаемый throughput |
|---|---|
| LocalQueue, virtual threads, no semaphore | 500k–1M msg/sec |
| LocalQueue, semaphore=50 | 50 × handler throughput |
| Kafka out, acks=all, single producer | 50k–200k msg/sec |
| Kafka in, single consumer thread | 10k–50k msg/sec |

**Проблемы производительности:**

1. **`LocalQueueInTransport` читает из очереди строго последовательно.** Нет batch-чтения (`drainTo()`), нет concurrent consumers из одной очереди.
2. **`BroadcastKafkaOutTransport` вызывает `partitionsFor()` на каждый send.** Нужно кэшировать `List<PartitionInfo>` и инвалидировать по исключению.
3. **`RetryEventPublisher` использует `ForkJoinPool.commonPool()`** для retry scheduling. При интенсивном retry конкурирует с остальными async задачами. Нужен dedicated `ScheduledExecutorService`.
4. **`SpringEventListenerRegistry` сканирует все бины на каждый `ContextRefreshedEvent`.** В большом приложении (500+ бинов) — O(N) на каждый refresh.
5. **`CopyOnWriteArrayList` для handler-списков** — правильно для read-heavy, но любая регистрация создаёт полную копию. Проблема при динамической регистрации в runtime.
6. **Нет батчинга на уровне publisher API.** Каждый `publish()` — отдельный вызов, без возможности batch-отправки через API библиотеки.

---

## 5. Безопасность

### Оценка: **7/10**

**Сильные стороны:**

- `EventPolymorphicTypeValidator` закрывает Jackson deserialization gadget-атаки (CVE-2017-7525 и серия). Двойная проверка: тип должен быть `Event`-подтипом + пакет в whitelist.
- `EventTypeRegistry` с пакетным whitelist — правильная модель: добавляешь пакет один раз, все классы из него разрешены автоматически.
- Сообщения об ошибке при отказе содержат инструкции по remediation — важно для security UX.
- Kafka producer настроен с `acks=all` + idempotent по умолчанию — защита от дублирования при сбоях.

**Проблемы:**

**Критические:**

1. **`EventTypeRegistry` — глобальный статический state без гарантии видимости при инициализации.** `allowPackage()` вызывается из Spring init thread, `isAllowed()` — из Kafka consumer thread. `ConcurrentHashSet` защищает сам set, но нет гарантии happens-before при старте.
2. **Kafka bootstrap servers хранятся в `event-flow.yml` в plaintext.** Нет поддержки SSL/TLS, SASL/SCRAM, mTLS из коробки. Пользователь может передать кастомные Kafka properties, но API это не документирует.
3. **`@JsonTypeInfo(use = CLASS)` — FQCN класса летит по сети в каждом сообщении.** Information disclosure: любой, кто читает топик, знает точную структуру классов продюсера.

**Умеренные:**

4. **Нет rate limiting на dispatcher.** `concurrency-limit` — backpressure на execution, но не защита от event flood. Нет circuit breaker.
5. **`KafkaInTransport` принимает события от любого producer.** Нет проверки source/origin. Whitelist защищает от gadget-атак, но не от бизнес-логики с фейковыми данными.
6. **Нет валидации содержимого события.** Bean Validation (JSR-380) не интегрирован. После десериализации событие сразу идёт в handler.

---

## 6. Тестируемость и сопровождаемость

### Оценка: **7/10**

- JaCoCo с минимальным порогом 60% — для публичной библиотеки низкая планка, рекомендуется 80%+.
- Integration test с `@EmbeddedKafka` — правильно, есть full round-trip тест.
- `clear()` в `EventTypeRegistry` как package-private костыль для тестов — признак того, что глобальный static state мешает тестированию.
- Отсутствие contract tests между core и spring модулями — изменение core API может сломать spring без явного сигнала.

---

## Итоговая таблица оценок

| Категория | Оценка | Ключевая проблема |
|---|---|---|
| **Архитектура и дизайн** | 7/10 | `Event.channels()` связывает домен с инфраструктурой |
| **Качество кода** | 7.5/10 | Глобальный static state, null из `@Bean`, баг в `KafkaInTransportFactory` |
| **Новизна подхода** | 4/10 | Нет новых идей, ниша уже занята Spring Cloud Stream / Axon |
| **Производительность** | 6.5/10 | Однопоточный consumer, нет batch, `partitionsFor()` на каждый send |
| **Безопасность** | 7/10 | Нет SSL из коробки, FQCN в сообщениях, нет валидации |
| **Тестируемость** | 7/10 | 60% coverage bar, static state ломает параллельные тесты |
| **Средняя оценка** | **6.5/10** | |

---

## Чёткие рекомендации

### Критично (исправить до production)

**1. Баг: `KafkaInTransportFactory` игнорирует `serializerFactory`**

```java
// event-flow-spring/.../KafkaInTransportFactory.java
// Сейчас:
return new KafkaInTransport(config.getServers(), config.getTopic(), config.getConsumerGroup());
// Должно быть:
return new KafkaInTransport(config.getServers(), config.getTopic(), config.getConsumerGroup(), serializerFactory);
```

**2. Убрать `Event.channels()` из доменного интерфейса.** Вместо этого — внешний routing реестр или аннотация без импорта инфраструктурных типов:

```java
@RouteTo({"internal", "external"})  // строки, не классы
public record OrderCreatedEvent(...) implements Event {}
```

**3. Убрать статический `EventTypeRegistry`, перейти на экземпляр через DI:**

```java
EventTypeRegistry registry = new EventTypeRegistry(List.of("com.example.events"));
// передавать в EventSerializerFactory / ObjectMapper конфигурацию
```

**4. Исправить `PublisherConfiguration` — не возвращать `null` из `@Bean`.** Бросать `BeanCreationException` с внятным сообщением или использовать `@ConditionalOnProperty`.

### Важно (исправить в ближайшем релизе)

**5. Добавить SSL/SASL поддержку для Kafka транспортов:**

```yaml
- name: kafka
  servers: kafka:9092
  security:
    protocol: SASL_SSL
    sasl-mechanism: SCRAM-SHA-256
    # credentials из environment variables, не из YAML
```

**6. Кэшировать `partitionsFor()` в `BroadcastKafkaOutTransport`:**

```java
private volatile List<PartitionInfo> cachedPartitions;
// обновлять при ProducerFencedException / InvalidMetadataException
```

**7. Заменить ForkJoinPool в `RetryEventPublisher` на dedicated `ScheduledExecutorService`:**

```java
private static final ScheduledExecutorService RETRY_SCHEDULER =
    Executors.newSingleThreadScheduledExecutor(
        Thread.ofVirtual().name("event-retry-", 0).factory()
    );
```

**8. Удалить `AbstractEventDispatcher`** — мёртвый код, создаёт путаницу в API.

### Желательно (для роста проекта)

**9. Поднять JaCoCo порог до 80%** и добавить mutation testing (PIT).

**10. Добавить batch publishing API:**

```java
CompletableFuture<SendResults> publishAll(List<Event> events);
```

**11. Добавить concurrent consumers для `LocalQueueInTransport`** — параметр `consumers: N` в конфигурации транспорта.

**12. Добавить симметричный in-транспорт для broadcast-подписки** (`BroadcastKafkaInTransport`).

**13. Заменить `@JsonTypeInfo(use = CLASS)` на реестр коротких имён** для уменьшения information disclosure и размера сообщений:

```java
// Вместо "com.example.OrderCreatedEvent" в JSON
// Использовать: "order.created" → Class mapping
```

---

## Общий вердикт

Event Flow — **добротная учебно-демонстрационная библиотека**, написанная с хорошим вкусом и вниманием к деталям (правильная конфигурация Kafka producer, backpressure через Semaphore, security whitelist). Виден опыт автора.

Однако для серьёзного production-использования библиотека **не предлагает ничего, что не покрывалось бы Spring Cloud Stream** (если нужен Kafka) или чистым **Spring `ApplicationEventPublisher` + `@TransactionalEventListener`** (если нужен in-JVM bus). Ниша существует, но узкая.

Главная задача для роста проекта — **исправить архитектурную ошибку с `Event.channels()`** (это сдерживает любое серьёзное DDD-применение) и **добавить production-grade Kafka security** (без этого нельзя использовать в корпоративной среде). Без этих двух вещей проект останется в категории "интересная демонстрация подхода".
