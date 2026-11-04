# ADR-0004 Implementation Plan: Two-Domain Event Retry

- **Status:** In progress (planning)
- **Date:** 2026-11-01
- **Related ADR:** [ADR-0004](../adr/0004-two-domain-event-retry.md)
- **Branch:** `feature/two-domain-event-retry`
- **Modules:** `event-flow-core`, `event-flow-spring`
- **Target version:** `1.4.0`

This is a working document. Update the checkboxes and the progress table as work advances. Keep the ADR as the source of truth for decisions; this document is the execution plan.

> **Execution rule:** after completing each task, stop and wait for explicit confirmation from the project owner before starting the next task. Do not implement several tasks in a row without confirmation.

## Task execution protocol

- One task from this plan equals one commit. Do not combine tasks in a single commit.
- Every functional task includes its own tests in the same commit. The consolidated test lists below are coverage checklists, not separate tasks.
- Before reporting a task as done, run the relevant module tests and the full suite when the change is cross-cutting.
- Report what changed, which tests were added and run, the results, and any risks or open questions.
- **Stop and wait for explicit confirmation from the project owner.** Do not start the next task and do not create the commit until confirmation is given.
- After confirmation, create exactly one commit for that task and update its checkbox in this document in the same commit.
- Do not start the next task until the previous task's commit is confirmed.
- Documentation-only or build-only tasks do not need new tests, but the full suite must still pass.

Commit message convention:

```text
feat(core): <task outcome>
fix(core): <task outcome>
docs: <task documentation>
```

Prefer a single commit per task that contains both the implementation and its tests.

---

## 1. Purpose

Implement ADR-0004 in small, independently reviewable PRs. Each PR must leave the project buildable, testable and backward compatible.

The implementation separates two retry domains:

- `DELIVERY` — publisher → consumer acceptance, confirmed by `AcceptedAck`;
- `PROCESSING` — handler execution after acceptance, confirmed by `SuccessAck` / `FailureAck`.

A missing handler is a permanent `NO_HANDLER` failure and goes to `DEAD` without automatic retry.

---

## 2. Fixed decisions for the implementation

These are locked before coding starts.

### 2.1 Ack contracts (final)

All acks implement `LifecycleAckEvent` and `TraceableEvent`. They are technical events: not persisted and they receive no lifecycle acks.

`RetryKind` lives in `io.github.vovten.eventflow.retry`:

```java
public enum RetryKind {
    DELIVERY,
    PROCESSING
}
```

`AcceptedAck`:

```java
public record AcceptedAck(
        UUID eventId,
        UUID originalEventId,
        String eventType,
        String originalService,
        int attempt,
        long maxProcessingMs,
        List<Class<? extends EventChannel>> sourceChannels,
        Instant occurredAt
) implements LifecycleAckEvent, TraceableEvent {

    public AcceptedAck {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(originalEventId, "originalEventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(sourceChannels, "sourceChannels must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
        if (maxProcessingMs < 0) {
            throw new IllegalArgumentException("maxProcessingMs must be >= 0");
        }
    }
}
```

`NoHandlerAck`:

```java
public record NoHandlerAck(
        UUID eventId,
        UUID originalEventId,
        String eventType,
        String originalService,
        int attempt,
        List<Class<? extends EventChannel>> sourceChannels,
        Instant occurredAt
) implements LifecycleAckEvent, TraceableEvent {

    public NoHandlerAck {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(originalEventId, "originalEventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(sourceChannels, "sourceChannels must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
    }
}
```

`FailureAck` (new components appended after the existing ones):

```java
public record FailureAck(
        UUID eventId,
        UUID originalEventId,
        String eventType,
        String originalService,
        String error,
        List<Class<? extends EventChannel>> sourceChannels,
        UUID processId,
        Instant occurredAt,
        RetryKind retryKind,
        int attempt,
        boolean retryable,
        List<String> failedHandlers,
        String errorType
) implements LifecycleAckEvent, TraceableEvent {

    public FailureAck {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(originalEventId, "originalEventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(error, "error must not be null");
        Objects.requireNonNull(sourceChannels, "sourceChannels must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        Objects.requireNonNull(retryKind, "retryKind must not be null");
        Objects.requireNonNull(failedHandlers, "failedHandlers must not be null");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
        failedHandlers = List.copyOf(failedHandlers);
    }

    // Legacy constructor retained for backward compatibility.
    public FailureAck(UUID eventId,
                      UUID originalEventId,
                      String eventType,
                      String originalService,
                      String error,
                      List<Class<? extends EventChannel>> sourceChannels,
                      UUID processId,
                      Instant occurredAt) {
        this(eventId, originalEventId, eventType, originalService, error,
                sourceChannels, processId, occurredAt,
                RetryKind.PROCESSING, 1, true, List.of(), null);
    }
}
```

`SuccessAck` (new components appended after the existing ones):

```java
public record SuccessAck(
        UUID eventId,
        UUID originalEventId,
        String eventType,
        String originalService,
        List<Class<? extends EventChannel>> sourceChannels,
        UUID processId,
        Instant occurredAt,
        RetryKind retryKind,
        int attempt
) implements LifecycleAckEvent, TraceableEvent {

    public SuccessAck {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(originalEventId, "originalEventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(sourceChannels, "sourceChannels must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        Objects.requireNonNull(retryKind, "retryKind must not be null");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
    }

    // Legacy constructor retained for backward compatibility.
    public SuccessAck(UUID eventId,
                      UUID originalEventId,
                      String eventType,
                      String originalService,
                      List<Class<? extends EventChannel>> sourceChannels,
                      UUID processId,
                      Instant occurredAt) {
        this(eventId, originalEventId, eventType, originalService,
                sourceChannels, processId, occurredAt,
                RetryKind.PROCESSING, 1);
    }
}
```

Field semantics:

- `attempt` is 1-based and counts attempts within the ack's retry context.
- In `SuccessAck` / `FailureAck`, `retryKind` is the **context of the dispatch that produced the ack**, not the semantic domain of the ack: `DELIVERY` when the event was re-published by the delivery scheduler, `PROCESSING` when re-published by the processing scheduler.
- A first publish has no retry context and defaults to `PROCESSING` / `attempt = 1`.
- `AcceptedAck` is always delivery and carries only the delivery attempt.
- `maxProcessingMs = 0` means "unknown; use the global processing timeout". PR 1 always sends `0`; PR 2 computes the real value.
- `retryable` is meaningful for `PROCESSING` acks. The legacy default is `true` to preserve old behaviour.
- `failedHandlers` contains handler `name()` values; it is empty for delivery acks.
- `errorType` is the fully qualified class name of the outermost exception, or `null` when unknown.
- `error` remains the human-readable error chain/message.
- `originalService` may be `null` (existing ack behaviour).
- `AcceptedAck` and `NoHandlerAck` intentionally omit `processId` in PR 1; `originalEventId` is sufficient for lifecycle correlation.
- `AcceptedAck` and `NoHandlerAck` must be added to the default logging exclusions.

### 2.2 Status semantics

| Status | Meaning |
|---|---|
| `UNDEFINED` | persisted only, not lifecycle-tracked |
| `NEW` | prepared for publication |
| `PUBLISHED` | sent to transport, awaiting `AcceptedAck` |
| `ACCEPTED` | consumer accepted, awaiting final ack |
| `HANDLED` | all handlers succeeded |
| `FAILED` | failure; retry details are in `event_retry` |
| `DEAD` | terminal dead-letter status; no automatic retry remains; manual `markForRetry` only |

`DEAD` is intentionally the same term as `RetryState.DEAD`: both mean dead-lettered/terminal. The longer name `DEAD_LETTERED` is not used.

### 2.3 Retry table invariants

- `event_retry` is keyed by `(event_id, retry_kind)`.
- `retry_kind` is `DELIVERY` or `PROCESSING`.
- `state` is `PENDING`, `IN_PROGRESS` or `DEAD`.
- If a retry is pending, a row exists; if no retry is pending, it does not (a `DEAD` row is retained for diagnostics).
- `attempt` is 1-based and counts attempts for that kind.
- Claims use `claimed_by` + `claimed_until`; an expired lease returns the row to `PENDING`.

### 2.4 Policy separation

- Processing policy: `@RetryPolicy` on listeners, named policies, configuration overrides.
- Delivery policy: publisher configuration only.
- No schedule is transported from consumer to publisher in an ack.

### 2.5 Allowed status transitions

`transitionStatus(eventId, allowedSources, target, error)` must update the event only when its current status is in `allowedSources`. A rejected transition is logged and ignored.

| Trigger | Allowed source statuses | Target |
|---|---|---|
| `AcceptedAck` | `NEW`, `PUBLISHED` | `ACCEPTED` |
| `NoHandlerAck` | `NEW`, `PUBLISHED`, `ACCEPTED`, `FAILED` | `DEAD` |
| `SuccessAck` | `NEW`, `PUBLISHED`, `ACCEPTED`, `FAILED` | `HANDLED` |
| `FailureAck(retryable = false)` | `NEW`, `PUBLISHED`, `ACCEPTED`, `FAILED` | `DEAD` |
| `FailureAck(retryable = true)` | `NEW`, `PUBLISHED`, `ACCEPTED`, `FAILED` | `FAILED` |
| publish success | `NEW`, `FAILED` | `PUBLISHED` |
| publish failure | `NEW`, `FAILED` | `FAILED` |
| legacy scheduler re-publish | `FAILED`, `PUBLISHED`, `ACCEPTED` | `NEW` |
| manual `markForRetry` | any non-terminal | `FAILED` / `NEW` |
| manual retry after `DEAD` | `DEAD` | `FAILED` / `NEW` |

Terminal statuses are `HANDLED` and `DEAD`. Acks never overwrite them; only an explicit operator action may resurrect a `DEAD` event.

Important: this is an explicit allowed-source set, not a numeric rank.

Out-of-order ack rules:

- Ack order is not guaranteed. `SuccessAck` may arrive before `AcceptedAck`.
- `SuccessAck`/`FailureAck`/`NoHandlerAck` prove delivery and must clear the `DELIVERY` row.
- `SuccessAck` before `AcceptedAck` → `HANDLED`; the late `AcceptedAck` is ignored for status, and no `DELIVERY` row remains.
- `FailureAck` before `AcceptedAck` → `FAILED` + `PROCESSING` row; the late `AcceptedAck` only clears the `DELIVERY` row and does not change the status.
- `AcceptedAck` from `FAILED` never transitions to `ACCEPTED`; it only clears the delivery row.
- A late publish result must not overwrite `HANDLED`, `ACCEPTED` or `DEAD`.
- A `SuccessAck` with `retryKind = DELIVERY` closes the delivery row and marks the event `HANDLED`.
- A `FailureAck` with `retryKind = DELIVERY` closes the delivery row and creates a `PROCESSING` retry row.
- A stale ack whose `attempt` is older than the current retry row is ignored.

### 2.6 Configuration invariant: `PROCESSING` requires `DELIVERY`

| `delivery.enabled` | `processing.enabled` | Mode |
|---|---|---|
| `false` | `false` | New retry mechanism off; legacy scheduler may run |
| `true` | `false` | Delivery-only mode; processing failures are recorded but not auto-retried |
| `true` | `true` | Full two-domain mode |
| `false` | `true` | **Invalid; fail fast at startup** |

Rules:

- `processing.enabled = true` with `delivery.enabled = false` fails startup with an `IllegalStateException`.
- The validation runs before any retry scheduler or `LifecycleRepository` is created.
- Implement it in a small `RetryConfigurationValidator` and unit-test it.
- In delivery-only mode: `AcceptedAck` and delivery retries work; `FailureAck` sets `FAILED` without a `PROCESSING` row; the processing watchdog is disabled; the legacy scheduler must be off.
- `ACCEPTED` belongs to the processing domain. It is **never** retried by the delivery layer: `DeliveryRetryScheduler` only selects `DELIVERY` rows. In delivery-only mode `ACCEPTED` is not retried at all and stays `ACCEPTED` if the final ack is lost.
- The legacy `EventRetryScheduler`'s `ACCEPTED` scan is a PR 1 transitional safety net. It is disabled together with the legacy scheduler when the two-domain mode is enabled.
- In both-disabled mode: the new mechanism is off; the legacy scheduler may run until it is removed.

---

## 3. PR sequence

| PR | Scope | Depends on | Main outcome |
|---|---|---|---|
| **PR 1** | Delivery/processing boundary | — | `AcceptedAck`, `NoHandlerAck`, new statuses, idempotency fix |
| **PR 2** | Local processing retry | PR 1 | `@RetryPolicy`, async local retry, failure classification |
| **PR 3a** | Durable retry store | PR 1 | `event_retry`, `LifecycleRepository`, transactional transitions |
| **PR 3b** | Durable retry schedulers | PR 3a | `DeliveryRetryScheduler`, `ProcessingRetryScheduler`, ack routing |
| **PR 4** | Operations and migration | PR 3b | Retention, metrics/logs, migration guide, docs |

PR 1 and PR 2 can overlap only after the ack contracts in section 2.1 are fixed. PR 3a cannot start before PR 1 because it needs `ACCEPTED`, `DEAD` and `RetryKind`.

---

## 4. Cross-cutting rules

- No breaking changes without default methods or retained legacy constructors.
- All new public API gets Javadoc and `@since 1.4.0`.
- Tests: JUnit 5, AssertJ, `@DisplayName`, matching existing style.
- Every PR runs:
  - `mvn -pl event-flow-core test`
  - `mvn -pl event-flow-spring test`
  - `mvn verify` (includes checkstyle)
- Documentation is updated in the same PR as the behaviour.
- Do not mix scope between PRs.
- **One task = one commit.** Each functional task includes its own tests in the same commit.
- **After completing a task, stop and wait for explicit confirmation.** Report what changed, which tests were added and run, and any risks. Create the commit only after confirmation.
- Do not combine tasks in one commit and do not start the next task before the previous commit is confirmed.

---

# 5. PR 1 — Delivery/processing boundary

**Goal:** introduce the delivery boundary and the permanent no-handler outcome. No retry table, no local retry loop.

**Task/commit rule:** each task below is a separate commit and includes its own tests.

## 5.1 Contracts

- [x] **1.1** Confirm the exact ack fields and legacy defaults from section 2.1.
- [x] **1.2** Add `RetryKind` enum in `io.github.vovten.eventflow.retry`:
  - `DELIVERY`, `PROCESSING`.
  - File: `event-flow-core/src/main/java/io/github/vovten/eventflow/retry/RetryKind.java`.
- [x] **1.3** Add `AcceptedAck`:
  - implements `LifecycleAckEvent`, `TraceableEvent`;
  - validates non-null fields;
  - `type()` returns `AcceptedAck.class`;
  - `channels()` returns `sourceChannels`.
  - File: `event-flow-core/src/main/java/io/github/vovten/eventflow/lifecycle/AcceptedAck.java`.
- [x] **1.4** Add `NoHandlerAck`:
  - same shape as `AcceptedAck` without `maxProcessingMs`.
  - File: `event-flow-core/src/main/java/io/github/vovten/eventflow/lifecycle/NoHandlerAck.java`.
- [x] **1.5** Extend `FailureAck`:
  - add `retryKind`, `attempt`, `retryable`, `failedHandlers`, `errorType`;
  - retain the legacy 8-argument constructor;
  - validate `attempt >= 1` and non-null `failedHandlers`.
  - File: `lifecycle/FailureAck.java`.
- [x] **1.6** Extend `SuccessAck`:
  - add `retryKind`, `attempt`;
  - retain the legacy 7-argument constructor.
  - File: `lifecycle/SuccessAck.java`.

## 5.2 Statuses and store

- [x] **1.7** Extend `EventStatus`:
  - `ACCEPTED('A')`;
  - `DEAD('D')`;
  - update Javadoc and the state diagram.
- [x] **1.8** Update status comments:
  - `event-store-postgresql.sql`;
  - `event-store-h2.sql`;
  - `event-store-mysql.sql`;
  - `event-store-oracle.sql`;
  - `event-store-mssql.sql`;
  - `SchemaInitializer` column comments.
- [x] **1.9** Add `EventStore.transitionStatus(...)`:
  - default method delegates to `updateStatus` with a Javadoc warning that the default is non-atomic;
  - `JdbcEventStore` overrides with `UPDATE ... WHERE event_id = ? AND status IN (...)`;
  - `InMemoryEventStore` overrides with `computeIfPresent` and an explicit allowed-source set;
  - a rejected transition is logged and ignored, not thrown;
  - `FAILED → ACCEPTED` is **not** allowed; a late `AcceptedAck` from `FAILED` only clears the `DELIVERY` row (wired in PR 3b).
  - Files: `EventStore.java`, `JdbcEventStore.java`, `InMemoryEventStore.java`.
- [x] **1.9a** `EventLifecyclePublisher.updatePublishResult`:
  - use `transitionStatus` for publish success/failure (`NEW`/`FAILED` → `PUBLISHED`/`FAILED`);
  - a late publish result must not overwrite `HANDLED`, `ACCEPTED` or `DEAD`.
  - File: `lifecycle/EventLifecyclePublisher.java`.

## 5.3 Dispatcher and ack handling

- [x] **1.10** `EventLifecycleDispatcher`:
  - keep `LifecycleAckEvent` pass-through;
  - for `MANAGED` + `TraceableEvent`, publish `AcceptedAck` before `origin.dispatch()`;
  - `maxProcessingMs = 0` in PR 1 (real value arrives in PR 2);
  - after dispatch:
    - `results.isEmpty() && !results.isDuplicate()` → `NoHandlerAck`;
    - otherwise existing `SuccessAck` / `FailureAck` logic;
  - `SuccessAck` / `FailureAck` echo `retryKind` and `attempt` from Envelope metadata;
  - when the metadata is absent, default to `PROCESSING` / `attempt = 1`;
  - a failure to publish `AcceptedAck` is logged but does not fail the dispatch.
  - File: `lifecycle/EventLifecycleDispatcher.java`.
- [x] **1.11** `AckHandler`:
  - declare `AcceptedAck`, `NoHandlerAck`, `SuccessAck`, `FailureAck`;
  - `AcceptedAck` → `ACCEPTED` only from `NEW`/`PUBLISHED`; from `FAILED` the status is unchanged (the `DELIVERY` cleanup is wired in PR 3b);
  - `NoHandlerAck` → `DEAD`;
  - `SuccessAck` → `HANDLED`;
  - `FailureAck` → `FAILED`;
  - all transitions use `transitionStatus` with the allowed source statuses;
  - terminal statuses (`HANDLED`, `DEAD`) are never overwritten by late acks.
  - File: `lifecycle/AckHandler.java`.
- [x] **1.12** `IdempotentEventDispatcher`:
  - invalidate the cache when `results != null && !results.isAllSuccess()`;
  - keep invalidation on `throwable != null`;
  - do not change duplicate handling.
  - File: `dispatcher/IdempotentEventDispatcher.java`.
- [x] **1.13** `EventRetryScheduler`:
  - temporarily include `ACCEPTED` in the legacy status list;
  - this preserves recovery of a lost final ack until the durable watchdog exists in PR 3b;
  - the scan is transitional and must be disabled together with the legacy scheduler when the two-domain mode is enabled.
  - File: `lifecycle/EventRetryScheduler.java`.
- [x] **1.13a** Update logging exclusions:
  - add `AcceptedAck` and `NoHandlerAck` to the default `excludedEvents`;
  - `EventFlowProperties.LoggingConfig`;
  - both `logging.excluded-events` sections in `event-flow.yml` (publisher and dispatcher);
  - update the Javadoc comment that lists the defaults.
  - Files: `EventFlowProperties.java`, `event-flow.yml`.

## 5.4 Test coverage checklist (not a separate task)

Tests for each task are written in the same commit as the task. This list is the minimum coverage checklist.

- [x] **1.14** `EventLifecycleDispatcherTest`:
  - `AcceptedAck` is published before handlers run;
  - an empty registry emits `AcceptedAck` and then `NoHandlerAck`;
  - a duplicate result does not produce `NoHandlerAck`;
  - `LifecycleAckEvent` passes through.
- [x] **1.15** `AckHandlerTest`:
  - `AcceptedAck` → `ACCEPTED`;
  - `NoHandlerAck` → `DEAD`;
  - a late `AcceptedAck` does not roll back `HANDLED`;
  - a late `AcceptedAck` after `FailureAck` keeps `FAILED`;
  - a late `FailureAck` does not roll back `DEAD`;
  - foreign-service acks are still ignored.
- [x] **1.16** `IdempotentEventDispatcherDefectTest`:
  - partial failure invalidates the cache;
  - the next dispatch actually invokes the handler.
- [x] **1.17** `EventStatusTest`:
  - codes `A` and `D`;
  - `fromCode`.
- [x] **1.18** `JdbcEventStoreTest` / `InMemoryEventStoreTest`:
  - allowed `transitionStatus`;
  - rejected `transitionStatus` leaves the row unchanged;
  - concurrent transition safety.
- [x] **1.18a** `EventLifecyclePublisherTest`:
  - a late publish result does not overwrite `HANDLED`, `ACCEPTED` or `DEAD`.
- [x] **1.19** Verify old `FailureAck` / `SuccessAck` call sites compile through the legacy constructors.

## 5.5 Documentation and build

- [x] **1.20** README: ack flow and new statuses `A`, `D`.
- [x] **1.21** CHANGELOG: new technical ack, new statuses, idempotency fix.
- [x] **1.22** Javadoc for all new public types.
- [x] **1.23** Run `mvn -pl event-flow-core test`.
- [x] **1.24** Run `mvn -pl event-flow-spring test`.
- [x] **1.25** Run `mvn verify` (checkstyle).
- [x] **1.26** Confirm no new compiler warnings.

## 5.6 Definition of Done

- [x] Publish → `AcceptedAck` → `ACCEPTED`.
- [x] No handler → `NoHandlerAck` → `DEAD`.
- [x] Partial failure does not block reprocessing.
- [x] Late acks cannot roll back terminal statuses.
- [x] No DDL changes beyond status comments.
- [x] All existing tests pass.
- [x] No breaking changes for custom `EventStore` implementations or user acks.

## 5.7 Out of scope for PR 1

- `@RetryPolicy`;
- changes to `UnifiedEventDispatcher` retry behaviour;
- `event_retry`;
- new schedulers;
- changes to `EventLifecyclePublisher` beyond status handling;
- per-handler targeting, DLQ, policy table.

## 5.8 Risks for PR 1

| Risk | Mitigation |
|---|---|
| Extra technical traffic from `AcceptedAck` | Emit only when lifecycle tracking is already enabled |
| Race between `AcceptedAck` and `updatePublishResult` | Monotonic `transitionStatus` |
| Changed duplicate behaviour | Dedicated tests for duplicate and partial failure |
| Custom `EventStore` lacks atomic transitions | Default method + documented degradation |

---

# 6. PR 2 — Local processing retry

**Goal:** add per-listener `@RetryPolicy`, an asynchronous local retry loop, and `permanent` / `transient-exhausted` classification.

**Task/commit rule:** each task below is a separate commit and includes its own tests.

## 6.1 Policy API

- [x] **2.1** Create the `io.github.vovten.eventflow.retry` package with:
  - `@RetryPolicy`;
  - `ResolvedRetryPolicy` (including `localMaxRetries`, local/durable attempt helpers);
  - `RetryPolicyRegistry`;
  - `RetryPolicyResolver` (declared named, referenced named, effective/default resolution);
  - `FailureKind` (`NONE`, `PERMANENT`, `TRANSIENT_EXHAUSTED`);
  - `RetryDelayScheduler`.
- [x] **2.2** `@RetryPolicy` attributes:
  - `name` — required; `METHOD` declares+binds, `TYPE` declares only;
  - `retryOn[]`, `noRetryOn[]`;
  - `maxRetries` — total retries after the first attempt;
  - `localMaxRetries` — local retries before durable hand-off;
  - `initialDelay`, `maxDelay` (String, parsed at startup);
  - `multiplier`, `jitter`;
  - sentinel values inherit the default policy.
- [x] **2.2a** Define the configured default policy:
  - used for listeners without `@RetryPolicy` and for unset annotation attributes;
  - shipped values preserve legacy behaviour:
    - `maxRetries = 3`;
    - `localMaxRetries = 0` (durable-only);
    - `initialDelay = 30s`, `multiplier = 2.0`, `maxDelay = 10m`, `jitter = 0.5`;
    - empty `retryOn` (any exception).
- [x] **2.3** `@EventListener`: add `retryPolicy()` named reference.
  - A method-level `@RetryPolicy` and a `retryPolicy` reference together fail startup.
- [x] **2.4** Policy reference for programmatic handlers:
  - add `default String retryPolicy()` to `EventSubscriber`, returning a named policy reference;
  - `MethodInvokingEventHandler` carries the resolved policy from the method declaration/reference;
  - do not add a separate `RetryPolicyAware` abstraction.

## 6.2 Policy resolution

- [x] **2.5** `EventListenerRegistry`:
  - register method-level `@RetryPolicy` declarations by their required name;
  - method-level declarations are declaration+binding and are reusable by other listeners through `retryPolicy`;
  - type-level declarations are declaration-only and never bind implicitly, even on a listener class;
  - resolve `@EventListener.retryPolicy` by name;
  - carry the resolved policy in `MethodInvokingEventHandler`;
  - fail-fast on duplicate names, unknown references and declaration+reference ambiguity;
  - document that shared policies belong on a dedicated type.
- [x] **2.6** `SpringEventListenerRegistry`: reuse base resolution, do not duplicate it.
- [x] **2.7** `EventSubscriberRegistry`: resolve the name returned by `EventSubscriber.retryPolicy()`;
  - empty name → default policy;
  - unknown name → fail startup.

## 6.3 Dispatcher

- [x] **2.8** `UnifiedEventDispatcher`:
  - read `attempt` from Envelope metadata (default `1`);
  - asynchronous local retry loop while `attempt <= localMaxRetries` and the total budget remains;
  - each attempt is an executor task;
  - delay via `RetryDelayScheduler`;
  - semaphore acquired per attempt, not per loop;
  - backoff `min(initial × multiplier^(n-1), max)` plus full jitter;
  - cause-chain exception matching;
  - when local budget is exhausted but total budget remains, return a result with
    `retryAfterMs = policy.backoff(attempt)` and `policyName`;
  - when `localMaxRetries = 0`, do not sleep; hand off to durable immediately;
  - global local-attempt safety cap.
- [x] **2.9** `HandlerResult`:
  - `retryable`, `failureKind`, `attempts`, `policyName`, `retryAfterMs`.
- [x] **2.10** `HandlerResults`:
  - `hasRetryableFailure()`;
  - `hasOnlyPermanentFailures()`;
  - aggregate failed handler names.
- [x] **2.11a** Extend `FailureAck`:
  - add `retryAfterMs` and `policyName`;
  - legacy constructor defaults: `retryAfterMs = 0`, `policyName = null`;
  - keep the legacy constructor for backward compatibility.
- [x] **2.11** `EventLifecycleDispatcher`:
  - compute `maxProcessingMs` from the local portion of each handler policy;
  - populate `FailureAck.retryable`, `failedHandlers`, `errorType`, `retryAfterMs`, `policyName`.

## 6.4 Spring wiring

- [x] **2.12** Add `RetryPolicyRegistry` and default-policy beans.
- [x] **2.13** Add `event-flow.retry.processing.*` properties, including `default-policy.*` and named policy overrides.
- [x] **2.14** Pass the registry/default policy to Spring registries and the delay scheduler to the dispatcher.

## 6.5 Test coverage checklist (not a separate task)

Tests for each task are written in the same commit as the task. This list is the minimum coverage checklist.

- [x] **2.15** `RetryPolicyRegistryTest`: named, unknown, duplicate, reserved `default` name.
- [x] **2.16** `RetryPolicyResolverTest`: `retryOn` / `noRetryOn` / empty, `localMaxRetries`, default policy, duration parsing, declaration+reference ambiguity.
- [x] **2.17** `UnifiedEventDispatcherRetryTest`:
  - transient failure succeeds locally;
  - exhausted local budget returns `retryAfterMs`;
  - `localMaxRetries = 0` performs no local retry;
  - total budget splits into local + durable attempts;
  - permanent failure is not retried;
  - multiple handlers are isolated.
- [x] **2.18** `EventListenerRegistryRetryTest` and `SpringEventListenerRegistryTest`:
  - method-level declaration is bound to its listener and reusable by another listener;
  - a type-level declaration alone does not bind implicitly;
  - duplicate names and declaration+reference ambiguity fail startup.
- [x] **2.19** `EventLifecycleDispatcherTest`: `maxProcessingMs`, `retryable`, `retryAfterMs`, `policyName`.
- [x] **2.20** Verify a platform-thread worker is not blocked during backoff.
- [x] **2.21** `FailureAckTest`: new `retryAfterMs`/`policyName` fields and legacy constructor defaults.

## 6.6 Definition of Done

- [x] Transient failures are absorbed locally without touching the store when `localMaxRetries > 0`.
- [x] `localMaxRetries = 0` sends `FailureAck(retryable = true, retryAfterMs = ...)` without local delay.
- [x] The policy's total budget spans local and durable attempts.
- [x] Other handlers are not re-run for a local retry.
- [x] Exhausted budget produces `FailureAck(retryable = true)` with `retryAfterMs` and `policyName`.
- [x] Permanent failure produces `FailureAck(retryable = false)`.
- [x] Listeners without a declaration or reference use the default policy; shipped default preserves legacy behaviour.
- [x] Every declared policy is a named policy and is overridable by config.
- [x] Method-level declaration and `retryPolicy` reference together fail startup.
- [x] Duplicate names, unknown references and the reserved `default` name fail startup.
- [x] A method-level declaration is reusable by name; a type-level declaration does not bind implicitly.

## 6.7 Risks for PR 2

| Risk | Mitigation |
|---|---|
| Blocking worker threads | Async delay scheduler, per-attempt semaphore |
| Spring proxy classes | `ClassUtils.getUserClass` |
| Long backoff on shutdown | Cancel in-flight retry futures in `stop()` |
| Policy misconfiguration | Fail-fast at registration |

---

# 7. PR 3a — Durable retry store and repository

**Goal:** add `event_retry`, `EventRetryStore` and a transactional `LifecycleRepository`. No schedulers and no ack routing changes yet.

**Task/commit rule:** each task below is a separate commit and includes its own tests.

## 7.1 Retry store

- [x] **3a.1** New package `io.github.vovten.eventflow.lifecycle.retry`:
  - `RetryKind`, `RetryState`, `RetryReason`;
  - `RetryTask`;
  - `EventRetryStore`;
  - `JdbcEventRetryStore`;
  - `InMemoryEventRetryStore`;
  - `RetryTaskMapper`;
  - `RetrySchemaInitializer`.
  - [x] **3a.1a** retry model, `EventRetryStore` SPI and `InMemoryEventRetryStore` + tests.
  - [x] **3a.1b** `JdbcEventRetryStore`, `RetryTaskMapper`, `RetrySchemaInitializer` and SQL resources.
- [x] **3a.2** `EventRetryStore` methods:
  - `schedule`;
  - `findDue`;
  - `claim`;
  - `reschedule`;
  - `complete`;
  - `dead`;
  - `find`;
  - `deleteByEventIds`.
- [x] **3a.3** `JdbcEventRetryStore`:
  - optimistic claim via `UPDATE ... WHERE state = 'PENDING' AND next_attempt_at <= now`;
  - `claimed_by` / `claimed_until`;
  - dialect SQL via default methods on `SqlDialect`.
- [x] **3a.4** `RetrySchemaInitializer`:
  - `CREATE TABLE IF NOT EXISTS event_retry`;
  - index `(retry_kind, state, next_attempt_at)`;
  - index `(service, retry_kind, state, next_attempt_at)`;
  - column comments.
- [x] **3a.5** Add SQL resources:
  - `event-retry-postgresql.sql`;
  - `event-retry-h2.sql`;
  - `event-retry-mysql.sql`;
  - `event-retry-oracle.sql`;
  - `event-retry-mssql.sql`.

## 7.2 Lifecycle repository

- [x] **3a.6** `LifecycleRepository`:
  - `onPublishFailed`;
  - `onAccepted`;
  - `onProcessingFailed`;
  - `onHandled`;
  - `onNoHandler`;
  - `onDeadLetter`;
  - `markForRetry`.
- [x] **3a.7** `JdbcLifecycleRepository`:
  - one `Connection` per transition;
  - `event_store` + `event_retry` in one transaction;
  - rollback both on failure.
- [x] **3a.8** `InMemoryLifecycleRepository`:
  - synchronized/lock-based atomicity.
- [x] **3a.9** `EventStore`: package-private connection-aware helpers so the JDBC repository does not duplicate SQL.

## 7.3 Test coverage checklist (not a separate task)

Tests for each task are written in the same commit as the task. This list is the minimum coverage checklist.

- [x] **3a.10** `JdbcEventRetryStoreTest` on H2:
  - create table;
  - schedule;
  - claim;
  - lease expiry;
  - reschedule;
  - complete;
  - dead.
- [x] **3a.11** `InMemoryEventRetryStoreTest`: behavioural parity.
- [x] **3a.12** `JdbcLifecycleRepositoryTest`: atomicity and rollback.
- [x] **3a.13** `RetrySchemaInitializerTest`.
- [x] **3a.14** `SqlDialectTest`: retry SQL for all dialects.

## 7.4 Definition of Done

- [x] Table is created on H2 and all five dialects.
- [x] Two instances cannot claim the same row.
- [x] `onHandled` deletes the retry row and sets `HANDLED` atomically.
- [x] A failure on one write rolls back both.
- [x] No retry row is created for healthy events.

## 7.5 Risks for PR 3a

| Risk | Mitigation |
|---|---|
| Dialect differences | Default methods on `SqlDialect`, H2 integration tests plus SQL-string unit tests |
| UUID mapping | Reuse `UuidType` and model `RetryTaskMapper` on `StoredEventMapper` |
| No Spring transactions in core | Explicit `Connection` in `JdbcLifecycleRepository` |

---

# 8. PR 3b — Durable retry schedulers and ack routing

**Goal:** enable real delivery and processing retries, watchdogs, and the new `AckHandler` / `EventLifecyclePublisher` rules.

**Task/commit rule:** each task below is a separate commit and includes its own tests.

## 8.1 Schedulers

- [x] **3b.1** `DeliveryRetryScheduler`:
  - due `DELIVERY` → claim → re-publish;
  - never selects `ACCEPTED`; only `DELIVERY` rows;
  - wait for `AcceptedAck`;
  - timeout → `PENDING` + `attempt++`;
  - cap → retry row `DEAD` + event status `DEAD`.
- [ ] **3b.2** `ProcessingRetryScheduler`:
  - due `PROCESSING` → claim → re-publish with `retryKind = PROCESSING`;
  - use the row's `next_attempt_at` computed from `FailureAck.retryAfterMs`;
  - fall back to global processing backoff when the ack had no relative delay;
  - wait for final ack;
  - retryable failure → `PENDING` with the next policy-driven delay;
  - permanent/cap → `DEAD`.
- [ ] **3b.3** Shared `RetrySchedulerSupport`:
  - claim, lease, batch, jitter, logging;
  - group by `eventId` to avoid double publish.
- [ ] **3b.4** Watchdogs:
  - `PUBLISHED` older than `deliveryAckTimeout` → create/raise `DELIVERY`;
  - `ACCEPTED` older than `processingAckTimeout` → create/raise `PROCESSING`; only when `processing.enabled = true`.

## 8.2 Ack routing

- [ ] **3b.5** `AckHandler`:
  - use `LifecycleRepository`;
  - resolve the retry row by `(eventId, ack.retryKind)` and compare `ack.attempt` with the row attempt;
  - `AcceptedAck` → delete `DELIVERY`, set `ACCEPTED` only from `NEW`/`PUBLISHED`;
  - `AcceptedAck` from `FAILED` → only delete `DELIVERY`, leave status unchanged;
  - `SuccessAck(retryKind = DELIVERY)` → delete `DELIVERY`, set `HANDLED`;
  - `SuccessAck(retryKind = PROCESSING)` → delete `PROCESSING` and any `DELIVERY`, set `HANDLED`;
  - `FailureAck(retryKind = DELIVERY)` → delete `DELIVERY`, create `PROCESSING PENDING`;
  - `FailureAck(retryKind = PROCESSING)` → update `PROCESSING` (or `DEAD` when permanent);
  - any processing ack (`SuccessAck`/`FailureAck`/`NoHandlerAck`) → delete `DELIVERY`, because it proves delivery;
  - `NoHandlerAck` → event `DEAD` + `PROCESSING` row `DEAD`;
  - `FailureAck(retryable = true)` → `FAILED` + `PROCESSING PENDING`;
  - set `next_attempt_at = now + clamp(ack.retryAfterMs)`; if `retryAfterMs` is absent or `0`, use the global processing backoff;
  - store `ack.policyName()` in `event_retry.policy_name` for diagnostics;
  - `FailureAck(retryable = false)` → event `DEAD` + `PROCESSING` row `DEAD`;
  - if processing is disabled: `FailureAck` sets `FAILED` with error details and does **not** create a `PROCESSING` row; log WARN and increment a metric;
  - stale-ack guard by `attempt` / `retryKind`.
- [ ] **3b.6** `EventLifecyclePublisher`:
  - create `DELIVERY PENDING` on publish failure;
  - stamp `retryKind` and `attempt`;
  - never roll back statuses;
  - `require-accepted-ack = false` → publish success equals `ACCEPTED`.
- [ ] **3b.7** Add `RetryMetadata` constants for Envelope keys.
- [ ] **3b.8** `markForRetry`:
  - reset the row to `PENDING`;
  - set the event to `FAILED` / `NEW`;
  - atomically.

## 8.3 Spring wiring

- [ ] **3b.9** Create `EventRetryStore`, `LifecycleRepository`, and both schedulers as beans.
- [ ] **3b.10** Add `event-flow.retry.delivery.*` and `event-flow.retry.processing.*`.
- [ ] **3b.10a** Add `RetryConfigurationValidator`:
  - fail fast with `IllegalStateException` when `processing.enabled = true` and `delivery.enabled = false`;
  - run before any retry scheduler or `LifecycleRepository` is created;
  - unit-test the validator and the Spring startup failure.
- [ ] **3b.11** Disable the legacy `EventRetryScheduler` when the new mode is enabled, including its transitional `ACCEPTED` scan.
- [ ] **3b.12** Update `EventCleanupScheduler` for dead-letter retention.

## 8.4 Test coverage checklist (not a separate task)

Tests for each task are written in the same commit as the task. This list is the minimum coverage checklist.

- [ ] **3b.13** End-to-end through `LocalQueue` plus in-memory/H2 store.
- [ ] **3b.14** Delivery failure → retry → accepted.
- [ ] **3b.15** Retryable processing failure → retry → handled.
- [ ] **3b.16** Permanent failure → dead.
- [ ] **3b.17** No handler → dead.
- [ ] **3b.18** Lost `AcceptedAck` → delivery retry.
- [ ] **3b.19** Lost final ack → processing retry.
- [ ] **3b.20** Stale ack is ignored.
- [ ] **3b.21** Both retry kinds for one event produce one re-publish per cycle.
- [ ] **3b.22** Cap exhausts into `DEAD`.
- [ ] **3b.23** Two scheduler instances do not process the same row.
- [ ] **3b.24** `processing.enabled = true` + `delivery.enabled = false` fails startup.
- [ ] **3b.25** Delivery-only mode: a handler failure sets `FAILED` and creates no `PROCESSING` row; no processing scheduler runs.
- [ ] **3b.26** `SuccessAck` before `AcceptedAck` → `HANDLED`, late `AcceptedAck` ignored, `DELIVERY` row gone.
- [ ] **3b.27** `FailureAck` before `AcceptedAck` → `FAILED` with `PROCESSING` row; late `AcceptedAck` only clears `DELIVERY` and keeps `FAILED`.
- [ ] **3b.28** `SuccessAck(retryKind = DELIVERY)` closes the delivery row and sets `HANDLED`.
- [ ] **3b.29** `FailureAck(retryKind = DELIVERY)` closes the delivery row and creates a `PROCESSING` row.
- [ ] **3b.30** Stale ack with an older `attempt` is ignored.
- [ ] **3b.31** Delivery-only mode does not retry `ACCEPTED`; the event remains `ACCEPTED`.
- [ ] **3b.32** `DeliveryRetryScheduler` ignores `ACCEPTED` and creates no `DELIVERY` row for it.
- [ ] **3b.33** Enabling the two-domain mode disables the legacy `ACCEPTED` scan.
- [ ] **3b.34** `FailureAck.retryAfterMs` drives `event_retry.next_attempt_at`; missing/zero falls back to global backoff.
- [ ] **3b.35** `policyName` is persisted in `event_retry` for diagnostics.

## 8.5 Definition of Done

- [ ] Delivery and processing retries work independently.
- [ ] Lost acks never leave an event stuck in `PUBLISHED` or `ACCEPTED`.
- [ ] No infinite loops: a cap always ends in `DEAD`.
- [ ] Multi-instance claiming works.
- [ ] Legacy scheduler does not conflict with the new schedulers.
- [ ] All PR 1–3a tests still pass.

## 8.6 Risks for PR 3b

| Risk | Mitigation |
|---|---|
| Double publish when both kinds are due | Group by `eventId`, claim, single re-publish |
| Ack races | Monotonic transitions plus attempt guard |
| Processing timeout shorter than local backoff | Use `maxProcessingMs` from PR 2 |

---

# 9. PR 4 — Operations, migration and deferred features

**Goal:** make the mechanism production-ready and close backward compatibility.

**Task/commit rule:** each functional task below is a separate commit and includes its own tests; documentation-only tasks require a passing full suite.

- [ ] **4.1** Dead-letter retention:
  - separate `maxAge`;
  - cleanup never deletes a pending retry;
  - dead-letter rows are deleted together with the event.
- [ ] **4.2** Overview SQL view for `event_store + event_retry` in README/release notes.
- [ ] **4.3** Diagnostics without a hard Micrometer dependency:
  - structured logs;
  - optional `RetryMetricsListener` / events for integration.
- [ ] **4.4** Migration:
  - old `FAILED` rows → legacy heuristic or backfill;
  - mixed fleet: `require-accepted-ack = false` during transition;
  - old-to-new property mapping.
- [ ] **4.5** README:
  - new properties;
  - ack flow;
  - state machine;
  - `@RetryPolicy` examples;
  - migration guide.
- [ ] **4.6** CHANGELOG: version `1.4.0`, no breaking changes, legacy scheduler deprecated.
- [ ] **4.7** Update ADR-0004 status from `Proposed` to `Accepted` after PR 3b.
- [ ] **4.8** Optional deferred features:
  - per-handler targeting and `PARTIALLY_HANDLED`;
  - per-destination delivery;
  - `event_retry_policy` table;
  - DLQ event with payload.

## 9.1 Definition of Done

- [ ] Runbook exists: inspect retries, manual retry, dead-letter cleanup.
- [ ] Mixed-fleet scenario is covered by a test.
- [ ] Old `FAILED` events are not lost.
- [ ] Documentation and CHANGELOG are updated.

---

# 10. Verification matrix

| Scenario | Expected result |
|---|---|
| Publish exception | `FAILED` + `DELIVERY PENDING` |
| Publish ok, consumer accepted | `ACCEPTED`, `DELIVERY` deleted |
| Publish ok, `AcceptedAck` lost | watchdog → delivery retry |
| Transient handler failure | local retry → `SuccessAck` |
| Local budget exhausted | `FailureAck(retryable = true)` → `PROCESSING PENDING` |
| Permanent handler failure | `FailureAck(retryable = false)` → `DEAD` |
| No handler | `NoHandlerAck` → `DEAD` |
| Final ack lost | `ACCEPTED` + timeout → processing retry |
| `SuccessAck` before `AcceptedAck` | `HANDLED`; late `AcceptedAck` ignored; `DELIVERY` row gone |
| `FailureAck` before `AcceptedAck` | `FAILED` + `PROCESSING`; late `AcceptedAck` only clears `DELIVERY` |
| Late publish result after success | does not overwrite `HANDLED`/`ACCEPTED`/`DEAD` |
| `SuccessAck(DELIVERY, N)` | closes delivery row and sets `HANDLED` |
| `FailureAck(DELIVERY, N)` | closes delivery row and creates `PROCESSING` |
| First-publish ack without metadata | defaults to `PROCESSING` / `attempt = 1` |
| Stale ack | ignored |
| Cap exhausted | `DEAD` |
| Two scheduler instances | one row processed once |
| Partial failure + idempotency | retry runs, not duplicate |
| No `@RetryPolicy` | default policy applies (`localMaxRetries = 0` by default) |
| Method-level policy reused by another listener | resolves through the global registry by name |
| Type-level `@RetryPolicy` on a listener class without a reference | does not bind implicitly; default policy applies |
| Old `FAILED` rows | legacy heuristic or backfill |
| `localMaxRetries = 0` + handler failure | `FailureAck(retryable = true, retryAfterMs = policy.backoff)`; no local sleep |
| `localMaxRetries = 2`, `maxRetries = 5` | attempts 1–3 local, attempts 4–6 durable, then `DEAD` |
| `FailureAck.retryAfterMs` present | publisher sets `next_attempt_at = now + clamp(retryAfterMs)` |
| `FailureAck.retryAfterMs` absent/0 | publisher uses global processing backoff |
| `processing.enabled=true` + `delivery.enabled=false` | startup fails with `IllegalStateException` |
| Delivery-only mode + handler failure | `FAILED`, no `PROCESSING` row, no processing retry |
| Delivery-only mode + final ack lost | remains `ACCEPTED`; no processing retry |
| `DeliveryRetryScheduler` sees `ACCEPTED` | ignores it; no `DELIVERY` row created |
| Two-domain mode + legacy scheduler | legacy off, including the `ACCEPTED` scan |

---

# 11. Global risks and mitigations

1. **Scope creep.** Enforce the PR boundaries; keep D12 ADR-0004 out of PR 1–3.
2. **Duplicate delivery.** Accepted as at-least-once; mitigate with idempotency, `AcceptedAck` and attempt guards.
3. **Duplicate retry.** Claim/lease plus grouping by `eventId`.
4. **Infinite loops.** `maxDeliveryRetries`, `maxProcessingRetries`, `DEAD`.
5. **Blocked threads during local retry.** Async delay scheduler, per-attempt semaphore.
6. **Lost acks.** Watchdogs over `PUBLISHED` / `ACCEPTED`.
7. **Custom `EventStore` incompatibility.** Default methods and legacy constructors; full functionality requires implementing `EventRetryStore`.
8. **Migration.** Keep the legacy scheduler available during the transition.

---

# 12. Progress tracker

| Phase | Status | Commits | Notes |
|---|---|---|---|
| PR 1 — boundary | Complete | 19 commits | PR 1 done; next: PR 2 |
| PR 2 — local retry | Complete | 13 commits | PR 2 done; next: PR 3a |
| PR 3a — retry store | Complete | 3 commits | 3a.1 and 3a.6–3a.12 done; next: PR 3b |
| PR 3b — schedulers | In progress | 1 commit | 3b.1 done; next: 3b.2 |
| PR 4 — operations | Not started | — | Depends on PR 3b |

Update this table after each merged PR.

---

# 13. Task execution log

Record one row per task. A row is completed only after the task's commit is confirmed by the project owner.

| Task | Commit | Tests in the same commit | Confirmed |
|---|---|---|---|
| 1.1 Ack contracts | `dc39e57` | docs-only; baseline suite passed | Yes |
| 1.2 `RetryKind` | `d5807b9` | `RetryKindTest`; full core suite passed | Yes |
| 1.3 `AcceptedAck` | `8c6cc06` | `AcceptedAckTest`; full core suite passed | Yes |
| 1.4 `NoHandlerAck` | `f64dfb1` | `NoHandlerAckTest`; full core suite passed | Yes |
| 1.5 `FailureAck` | `2e73dab` | `FailureAckTest`; full core suite passed | Yes |
| 1.6 `SuccessAck` | `9676a6d` | `SuccessAckTest`; full core suite passed | Yes |
| 1.7 `EventStatus` | `a129a74` | `EventStatusTest`; full core suite passed | Yes |
| 1.8 Status comments | `3b26042` | docs-only; full core suite passed | Yes |
| 1.9 `transitionStatus` | `ed4ae8a` | store transition tests; full core suite passed | Yes |
| 1.10 Lifecycle dispatcher | `19be94e` | dispatcher ack tests; full core suite passed | Yes |
| 1.11 `AckHandler` | `97b7ac7` | `AckHandlerTest`; full core suite passed | Yes |
| 1.12 Idempotency cache | `aed048b` | idempotency tests; full core suite passed | Yes |
| 1.13 ACCEPTED retry scan | `6d0559b` | `EventRetrySchedulerTest`; full core suite passed | Yes |
| 1.13a Logging exclusions | `d7efb49` | `EventFlowPropertiesTest`; full spring suite passed | Yes |
| 1.9a Publish result transitions | `f033efc` | `EventLifecyclePublisherTest`; full verify passed | Yes |
| 1.20–1.26 Docs and verification | `8f3946f` | full `mvn verify` passed | Yes |
| 2.1–2.2a Policy API | `b93fdab` | policy API tests; full core suite passed | Yes |
| 2.1b Named-only policy rework | `be5fdd4` | policy API tests; full `mvn verify` passed | Yes |
| 2.3 `@EventListener.retryPolicy` | `0151aaf` | `EventListenerAnnotationTest`; full core suite passed | Yes |
| 2.4 `EventSubscriber.retryPolicy()` | `cdc1b72` | `EventSubscriberTest`; full core suite passed | Yes |
| 2.5 Listener registry policy resolution | `bfd91b6` | `EventListenerRegistryRetryTest`; full core suite passed | Yes |
| 2.5a Extract listener retry policy resolver | `aac0d39` | registry tests + full `mvn verify` passed | Yes |
| 2.6 Spring registry reuse | `a9d4c64` | `SpringEventListenerRegistryTest`; full `mvn verify` passed | Yes |
| 2.7 Subscriber policy resolution | `4273ca6` | `EventSubscriberRegistryRetryTest`; full `mvn verify` passed | Yes |
| 2.9 Handler result classification | `5adee1b` | `HandlerResultTest`/`HandlerResultsTest`; full core suite passed | Yes |
| 2.10 `HandlerResults` aggregates | `5adee1b` | delivered with 2.9; tested | Yes |
| 2.8 Dispatcher local retry loop | `2a60456` | `UnifiedEventDispatcherRetryTest`; full `mvn verify` passed | Yes |
| 2.11a–2.11 Policy-driven failure ack | `b856ed5` | ack/dispatcher tests + full `mvn verify` passed | Yes |
| 2.12–2.14 Spring wiring | `8f85376` | `RetryPolicyConfigurationTest`/`EventFlowPropertiesTest` + full `mvn verify` passed | Yes |
| PR 2 final tests/docs | `ee29e1e` | non-blocking retry test + full `mvn verify` passed | Yes |
| 3a.1a Retry model + in-memory store | `a29c66f` | `RetryTaskTest`/`InMemoryEventRetryStoreTest`; full core suite passed | Yes |
| 3a.1b JDBC retry store | `308b42c` | JDBC/schema/dialect tests + full `mvn verify` passed | Yes |
| 3a.6–3a.9, 3a.12 Lifecycle repositories | `60dbce0` | repository atomicity/rollback tests + concurrency test + full `mvn verify` passed | Yes |
| 3b.1 `DeliveryRetryScheduler` | `64f2979` | `DeliveryRetrySchedulerTest` (18 tests) + full `mvn verify` passed | Yes |
| 3b.1a Attempt numbering alignment | this commit | `DeliveryRetrySchedulerTest`/`RetryTaskTest`; full `mvn verify` passed | Yes |

Rules:

- One task = one commit.
- The commit contains the implementation and its tests.
- The checkbox in this plan is updated in the same commit.
- The next task does not start until the previous commit is confirmed.

---

# 14. Next step

Continue PR 3b with **3b.2 `ProcessingRetryScheduler`**: due `PROCESSING` rows, policy-driven next attempt, final-ack handling and dead-lettering.
