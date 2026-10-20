# План поиска дефектов и изъянов в `event-flow` (core + spring) 1.3.1

Методичная кампания по поиску ещё не известных дефектов в библиотеке — не повторение уже
исправленных (см. `event-flow-spring-example/docs/defects.md`). Цель каждой стадии —
выдать воспроизводимые дефекты в формате `docs/defects-<area>.md` (симптом → причина →
фикс → тест), путь к стабилизации библиотеки перед следующим релизом.

## Объект и метод

- **Объект**: `io.github.vovten:event-flow-core` + `event-flow-spring`, ~110 классов,
  ~15 300 строк, 9 подсистем core + 26 spring-классов автонастройки.
- **Методы** (комбинируем на каждой стадии):
  1. **Аудит кода подсистемы** — чтение по символам, чек-лист сбоев (nulls, races, leaks,
     ordering, error-masking, resource lifecycle).
  2. **Тест на гипотезу** — юнит/интеграционные тесты, воспроизводящие подозрение
     (AAA, юнит-тесты проверяют поведение, а не реализацию).
  3. **Стресс/параллельные тесты** — гонки, параллельные публикации/диспетчирование,
     конкурентность store/retry/cleanup.
  4. **E2E через `event-flow-spring-example`** — реальные Kafka + Postgres, checkstyle
     валиден, сценарии curl + KafkaProbe + запросы в `event_store`.
  5. **Статический анализ / компиляторные ворнинги** — `-Xlint`, потенциально spotbugs
     как разовая ревизия (не внедряем в сборку без согласия).

---

## Стадии (в порядке убывания риска)

### 1. Lifecycle-конвейер: publisher ↔ store ↔ retry ↔ ack (наивысший риск)

**Классы**: `EventLifecyclePublisher`, `EventLifecycleDispatcher`, `EventRetryScheduler`,
`EventCleanupScheduler`, `LifecycleResolver`, `AckHandler`, `SuccessAck/FailureAck`.

**Гипотезы для проверки:**

- Retry-цикл: сортировка «залипших» событий, cutoff `updated_at`, race с очисткой
  (cleanup может удалить событие, которое retry только что пере-публикует →
  «зомби»-события без строк истории).
- Квант batch retry: счётчик `maxRetries` при одновременных авто- и ручных ретраях
  (ADR-0002 упоминает двойной слой ретраев).
- Оборачивание raw-событий в `Envelope` (фикс `d95171f`): поведение raw `TraceableEvent`
  с null `eventId()`, двойной `pubSrv`-метадатой, дубли событий при перес-публикации.
- `EventCleanupScheduler` vs шедулер retry: race при очистке + интервалы/таймзоны UTC.
- Статусы: `N/U/P/H` — есть ли картина с `FAILURE`-ack на непокрывающем стеке; что
  происходит при отверженном FailureAck (loop Fail ack → Failure → Fail?).

**Итог**: `docs/defects-lifecycle.md` + регрессионные тесты в core.

### 2. Хранилище: JDBC store, DDL, диалекты, UTC

**Классы**: `JdbcEventStore`, `SchemaInitializer`, `StoredEventMapper`, `UuidType`,
`SqlDialect` + 5 диалектов, `InMemoryEventStore` (прототип для тестов — сверить семантику).

**Гипотезы:**

- Типы UUID (Postgres `uuid`/Oracle `RAW(16)`/SQL Server `UNIQUEIDENTIFIER`):
  потеря/обрезка/переворачивание байтов MSB/LSB (SQL Server, `byte order` уникальных
  идентификаторов).
- UTC-конверсия (было «ин принимается», тест мало проверил `h2/pg`).
- Размеры колонок (`varchar(512)` для `event_type`, `TEXT` для payload): как поведут
  себя большие payload (msgpack-сериализованные) на Oracle `CLOB` vs MySQL `LONGTEXT`.
- SQL-инъекции/экранирование в `table-name` (пользователь вводит `myschema.event_store`,
  дефолт-schema префикс — это известное поведение? автопроверка идентификаторов).
- Пагинация «старых» событий для retry: `LIMIT/OFFSET` vs число возвращённых строк.
- Индекс `idx_event_store_status` vs рабочий запрос cleanup/retry — после роста
  таблицы > 1 млн строк (см. 657К в тестах) возможны проблемы производительности.

**Итог**: `docs/defects-store.md` + интеграционный тест на Postgres (по аналогии с
JdbcEventStore Tests).

### 3. Dispatch-стек и идемпотентность

**Классы**: `UnifiedEventDispatcher`, `IdempotentEventDispatcher`, `EventDispatcherBuilder`,
`HandlerResult(s)`, backlog/параллелизм; spring-обёртки регистраторов.

**Гипотезы:**

- Идемпотентность по `TraceableEvent.eventId()`: что при raw non-traceable события
  (не Envelope) — дубли обработки? Что при re-dispatch через retry (тот же eventId)?
- Race дедупликации (тест «Race Condition Tests» покрывает AtomicInteger; проверить
  ключи/фора, память-лимиты (unbounded рост кеша идемпотентности?).
- Backpressure (есть тесты): квант очереди, поведение при тихом закрытии хэндлера
  (нимот exception) — теряется ли статус полувыполненного dispatch.
- Пере-доставка одному хэндлеру (broadcast + transport → тот же хэндлер = двойная
  обработка). Поведение при нескольких подписчиках одного типа события.
- Порядок delivery при broadcast: «все партиции» vs общий порядок; можно
  ли ожидание, что внешний/внутренний канал не смешиваются.

**Итог**: `docs/defects-dispatch.md`.

### 4. Serialization-граница (json/msgpack, safety)

**Классы**: `JsonEventSerializer`, `MsgPackEventSerializer`, `EventTypeRegistry`,
`EventPolymorphicTypeValidator`, `Secure Deserialization` тесты.

**Гипотезы:**

- `@class` полиморфизм: round-trip для `Envelope`-пейлоадов с вложенными коллекциями
  (List<Map<String,Object>> — Jackson не сохраняет типы), record-типов, enum,
  `LocalDateTime`/`Instant`, `UUID`.
- Разрешённые пакеты (`allowed-event-packages`): можно ли обойти проверку (подпакеты,
  схожие имена `com.example` vs `com.example2`, префиксное совпадение имён).
- MsgPack: регистрация типов, закрытие байтовых исходников (`Lexer/Parser` ресурсы).
- Циклы и self-reference в payload — StackOverflowError вместо понятного Exception?
- Payload > JVM heap: размерные литералы, шифрование больших строк.

**Итог**: `docs/defects-serialization.md`.

### 5. Kafka-транспорты (outgoing/incoming) и ресурсные контракты

**Классы**: `AbstractKafkaOutTransport`, `KafkaOutTransport`,
`BroadcastKafkaOutTransport`, `EventKeyResolver(Builder)`, `KafkaInTransport`,
factory бины spring-слоя.

**Гипотезы:**

- Ключи: резолвер бросает исключение — молча глотается (найдено в Дефекте 2); теперь
  Уровень логгирования, но fail-флоу на метадатных ключах.
- Broadcast: статический список партиций, зафиксированный при старте — новые партиции
  после ребаланса/расширения не получают копии (устаревший список).
- Метаданные записи (headers): теряются ли пользовательские headers при broadcast?
- KafkaInTransport: ack-режим, offset-закрутки при исключениях в dispatch (пере-обработка
  без бесконечного цикла), DLT-сценарии отсутствуют.
- Парти-обязательства: транзакционно? При `deliveryTimeout` fail-сигнал завершает
  transaction? (checkstyle да).
- Двойная конфигурация продюсера: `enable.idempotence` поверх маленький `max.in.flight`
  — ordering/latency конфигурации какие нетривиальные взаимодействия.

**Итог**: `docs/defects-kafka.md`.

### 6. Конфигурирование и Spring-автонастройка

**Классы**: `EventFlowProperties`, все `*Configuration`, `NoEventBeansCondition`,
фабрики transport'ов.

**Гипотезы:**

- Условия `ConditionalOn...`: конфликт дублирующих бинов `bean:` резолвера (custom
  резолвер + дефолт — кто выигрывает; проверка при нескольких `EventKeyResolver`).
- Deprecated конструкторы (см. `80a6704`): которые из них реально оживут без
  spring-boot-starter (classpath-различия).
- Валидация конфига при старте: `key: metadata:product-id` на broadcast + отключён
  lifecycle — warn/error?
- `disabled` режим «полный» (есть `EventFlowDisabledAutoConfigurationTest`): не
  создаёт пулов/потоков — утечка фоновых потоков по false-positive.
- Конфигурация retry: выключение `retry.enabled` -> шедулер не стартует, но накопленные
  застрявшие события без ретрая (дефект 1 — вылито как «включено по умолчанию»).

**Итог**: `docs/defects-spring-config.md`.

### 7. API-контракты и Java-семантика (тип перегрузок, generics, Default-methods)

**Классы**: `EventPublisher` + `DefaultEventBuilder`, `Event`/`TraceableEvent`/
`AbstractTraceableEvent`, `Envelope`.

**Гипотезы:**

- Остатки «ловушки перегрузок»: `publish(UUID, T)` где T — Event; `publishEnvelope`
  для данных, собранных пользователем; автодокка может доматывать Envelope.
- `Envelope.of(null)` / `Envelope` внутри `Envelope` (защита уже есть, проверить
  реекцию наверх); нет ли потери данных при `Envelope.of` с типами по умолчанию.
- Согласованность UUID `eventId` в `prepare()` vs raw publish (когда и где генерится
  идентичность события).
- Небио-класс (POJO) без no-arg конструктора при десериализации — понятные ошибки.

**Итог**: дополнение к `docs/defects-lifecycle.md` или отдельный `defects-api.md`.

### 8. Наблюдаемость, логи, метрики

**Классы**: `LoggingEventPublisher/Dispatcher`, `EventLogUtils`, `JsonBuilder`.

**Гипотезы:**

- Логи утечек данных: payload печатается целиком (PII) с Error-уровнями?
- `JsonBuilder` — корректный escaping кириллицы/спецсимволов; поведение логов под
  Windows (CP1251).
- Нет метрик (micrometer) — замечание, не дефект; фиксация в АД-insight для FAQ.

**Итог**: `docs/defects-observability.md` (маленькая горячая зона).

---

## Протокол для каждого дефекта (формат работает — дефекты.md 1–3)

Каждый найденный дефект оформляется как отдельный раздел в `docs/defects-<area>.md`:

```markdown
## N 🔴/🟠/🟡 Короткое название
### Симптомы      — вывод логов/статусов, видно снаружи
### Причина       — точное место (класс/метод/строка-симптом), JLS/JLR ссылки
### Воспроизведение — минимальный сценарий (тест, curl, SQL/Kafka)
### Как исправить  — вариант A (библиотека) + B (обход) с оценками совместимости
```

Критичность: 🔴 потеря данных/шторм/недоступность, 🟠 сломана фича, 🟡 UX/надёжность.

## Порядок выполнения и выхода

| Стадия | Зона | Оценка времени |
|--------|------|----------------|
| 1 | Lifecycle | 1–2 сессии |
| 2 | Store/DB | 1 сессия |
| 3 | Dispatch | 1 сессия |
| 4 | Serialization | 1 сессия |
| 5 | Kafka | 1–2 сессии |
| 6 | Spring-config | 1 сессия |
| 7 | API-контракты | 0.5 сессии |
| 8 | Наблюдаемость | 0.5 сессии |

**Критерии прогресса:** каждая стадия завершается написанным docs-файлом (даже если
дефектов нет — «проверено, чисто») и дополняющими тестами в репозитории библиотеки,
проверяющими исправления и области. После всего — сводный `docs/defects-summary.md`
(с «Сводка»-таблицей и мультиспектр роста) и, по желанию, release-нота.
