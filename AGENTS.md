# Event Flow - Agent Guide

## Quick Start

- **Java 21+ required** (enforced by Maven compiler plugin)
- **Build**: `mvn clean package`
- **Test**: `mvn test` (runs All `*Test.java` and `*Tests.java`)
- **Code quality**: Checkstyle enabled at `validate` phase

## Monorepo Structure

| Module | Purpose |
|--------|---------|
| `event-flow-core/` | Framework-agnostic library (pure Java 21+) |
| `event-flow-spring/` | Spring Boot auto-configuration with YAML |

## Configuration

- **Default config**: `event-flow-spring/src/main/resources/event-flow.yml`
- **All components disabled by default** — explicitly enable via `event-flow.enabled: true`
- **Required properties**:
  - `event-flow.dispatcher.listener-packages` — package(s) to scan for `@EventListener`
  - At least one channel in `event-flow.publisher.channels`
  - At least one transport in `event-flow.dispatcher.transports`

## Default Configuration (event-flow.yml)

**Channels**:
- `internal` → `local-queue` (in-JVM)
- `external` → `kafka` (topic: `events`, servers: `localhost:9092`)
- `broadcast` → `broadcast-kafka` (topic: `events`)

**Transports**:
- `local-queue` (capacity: 1000)
- `kafka` (topic: `events`, consumerGroup: `event-flow-group`)
- `broadcast-kafka` (topic: `events`)

**Key defaults**:
- `publisher.transactional: true` — events sent after transaction commit
- `retry.enabled: true`, `max-attempts: 3`, `initial-delay: 100ms`, `multiplier: 2.0`
- Dispatcher uses **virtual threads** — thread pool settings ignored
- `concurrency-limit: 0` — no backpressure (set 20-100 for I/O-bound handlers)

## Architecture Highlights

- **EventChannel types**: `InternalEventChannel`, `ExternalEventChannel`, `BroadcastEventChannel`, `GenericEventChannel`
- **Transports**: `local-queue`, `kafka`, `broadcast-kafka`
- **Handler discovery**: `@EventListener` annotation, `EventSubscriber` interface
- **Serialization**: JSON (0x01), MessagePack (0x02), pluggable custom formats
- **Security**: `allowed-event-packages` whitelist (default: `io.github.vovten.eventflow`)

## Extension Points

- **Custom transport**: Implement `OutTransportFactory` or `InTransportFactory` + `@Component`
- **Custom serializer**: Implement `EventSerializer` + `@Component`
- **Custom channel**: Provide `EventChannel` bean (e.g., `BroadcastEventChannel`)

## Style & Conventions

- **Indentation**: Spaces only (enforced by Checkstyle)
- **Line endings**: LF (no Windows CRLF)
- **Package naming**: lowercase (`com.example.events`)
- **Class naming**: CamelCase (`OrderCreatedEvent`)
- **Method/field naming**: camelCase (`handleOrderCreated`)
- **Constants**: UPPERCASE_WITH_UNDERSCORE
- **Javadoc**: Required for public API (Checkstyle: `MissingJavadocMethod`)
- **Braces**: Always use `{}` even for single-line blocks
- **No Windows line endings** (Checkstyle: `RegexpMultiline`)

## Testing

- **.junit.jupiter** for tests
- **Mockito** available via `spring-boot-starter-test`
- **Kafka integration** via `spring-kafka-test` and `kafka_2.13`
- Test classes: `*Test.java`, `*Tests.java`

## Important Notes

1. **Default listener-packages** in `event-flow.yml`: `io.github.vovten.eventflow` (auto-config internal handlers)
2. **Default allowed-event-packages**: `io.github.vovten.eventflow` (security whitelist)
3. **Thread pool settings** are ignored when using virtual threads (Spring Boot default)
4. **Retry enabled** with exponential backoff (100ms, 2.0 multiplier)
5. **Transactional publishing** enabled by default (events published after commit)
