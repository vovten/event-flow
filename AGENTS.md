# AGENTS.md

Event Flow — lightweight framework for event-driven applications (publish, route, process events). Apache 2.0, published to Maven Central.

## Build & Test

- Maven multi-module: parent POM `event-flow` → `event-flow-core` (framework-agnostic Java 21) + `event-flow-spring` (Spring Boot auto-configuration, depends on core).
- Java 21 required. No wrapper; system `mvn` is used.
- **Do not run `mvn verify`** for local checks: it triggers GPG artifact signing and JaCoCo coverage gate (≥60% line coverage). CI runs `mvn -B package` (package < verify), use the same.
- Full test run: `mvn test` (checkstyle runs first in the `validate` phase and will fail the build on violations).
- Single test: `mvn -pl event-flow-core test -Dtest=EventDispatcherBuilderTest`. For spring module add `-am` (needs core in the reactor).
- Skip checkstyle if needed: `-Dcheckstyle.skip=true`; skip coverage: `-Djacoco.skip=true`.

## Style (enforced by Checkstyle at build time)

- Code must follow Clean Code principles (Robert C. Martin): small functions, intention-revealing names, no magic numbers, single responsibility.
- Config lives at `config/checkstyle/checkstyle.xml` + `suppressions.xml`; bound to the `validate` phase in both modules (test sources included).
- Javadoc is required on non-private classes and methods in `src/main` (suppressed for tests). `@param`/`@return`/`@throws` must be ordered and non-empty.
- Strict rules you will hit often: each annotation on its own line, `DeclarationOrder` (fields, ctors, methods), `NeedBraces`, no catching of `Throwable`, LF line endings (no CRLF), 4-space indent, `UpperEll` for longs.
- Tests may skip javadoc but still pass all other rules.

## Testing conventions

- JUnit 5 + AssertJ + Mockito + Awaitility (core), Spring Boot test + `spring-kafka-test` (spring). All tests carry `@DisplayName`.
- `Spring` integration test `ExternalPublisherDispatcherIntegrationTest` uses `@EmbeddedKafka` (in-JVM Kafka, no Docker); it is the slow, network-heavy one.
- Core has benchmark-style tests under `event-flow-core/src/test/java/.../benchmark/` (`KafkaBenchmarkTest`, `LocalQueueBenchmarkTest`) — slow, separate from unit tests despite the `*Test` suffix.
- H2 is available for core JDBC/lifecycle tests.

## Architecture invariants

- Root package: `io.github.vovten.eventflow.*`. Core packages: `channel`, `dispatcher`, `event`, `lifecycle`, `publisher`, `registry`, `serialization`, `transport`, `util`.
- **`event-flow-core` must stay framework-agnostic** — never add Spring dependencies there. All Spring deps in `event-flow-spring` are `<optional>true</optional>`.
- Serializers use a leading magic byte: `0x01` JSON, `0x02` MessagePack (reserved). Custom serializers start at `0x03` and must be registered in `EventSerializerFactory`.
- Deserialization is whitelist-gated via `EventTypeRegistry` (default allows `io.github.vovten.eventflow.*`); consumers can allow packages/classes.
- Built-in transports: `LocalQueue*Transport` (in-JVM, paired via `LocalQueueTransportsBuilder`), `Kafka*Transport`, `BroadcastKafkaOutTransport`.
- Lifecycle tracking DB is dialect-aware; DDL scripts shipped at `io/github/vovten/eventflow/lifecycle/store/db/event-store-<dialect>.sql`.

## Git conventions

- Conventional commits: `type(scope): subject`, scope is `core` or `spring`; types in use: `feat`, `fix`, `docs`, `test`, `refactor`, `release` (version bumps).
- Work happens on short-lived feature branches; `main` is the protected/release branch.
- Release flow (do not do casually): bump version in POMs, update `CHANGELOG.md`, tag, deploy via `-Pdeploy-core`/`-Pdeploy-spring`/`-Pdeploy-all` (Maven Central, GPG-signed).
- `flatten-maven-plugin` generates `.flattened-pom.xml` files (gitignored) — do not commit them.

## Change planning

- This repo uses OpenSpec: planned work lives in `openspec/changes`, skills/commands in `.opencode/`. Follow the `/opsx:*` workflow (explore → propose → apply → archive) before large changes.

## Docs

- `README.md` (root) = framework overview + API; `event-flow-core/README.md` and `event-flow-spring/README.md` = module specifics. Spring config reference default in `event-flow-spring/src/main/resources/event-flow.yml`.