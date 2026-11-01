# ADR-0001: Per-Listener Retry Policies, Scheduled Re-publish and Dead-Lettering

- **Status:** Proposed
- **Date:** 2026-10-18
- **Author:** Vladimir Aleshkov (designed with architecture review)
- **Deciders:** project maintainer
- **Tags:** lifecycle, retry, dead-letter, event-store, api

## Context

### What exists today

Event Flow already has three independent retry-related mechanisms:

| Mechanism | Side | Scope |
|---|---|---|
| `RetryEventPublisher` | publisher | Retries the **publish call** itself (broker/network failure), in-memory, exponential backoff |
| `EventRetryScheduler` | publisher | Scans `EventStore` (FAILED / PUBLISHED / NEW), **global** `maxRetries` + backoff `minAge × 2^retryCount`, re-publishes the whole event |
| `EventStore.markForRetry()` | API | Manual flag `retry = TRUE`; the scheduler retries bypassing limits and backoff |

### Gaps

1. `@EventListener` has a single attribute (`value()`) — there is **no way to declare a retry policy** for a listener.
2. There is **no classification of transient vs permanent failures**: every failed MANAGED event is retried identically.
3. When the retry budget is exhausted the scheduler only logs a warning; the event stays in `FAILED` forever (invisible "dead letter").
4. There is **no observable event** for processing failures — consumers cannot feed metrics, alerting, or a DLQ sink reactively.

### Driver

Users need to declare, per listener, **which exceptions warrant a re-publish** and **how the re-publish is scheduled** (delay, backoff, attempt budget), plus an event notification when processing fails. Reusable, named policies are required for the same policy across many listeners.

## Goals

- Exception-driven retry classification declared in code (`@RetryPolicy`), resolvable to a per-event schedule.
- Reusable named policies, not only inline declarations.
- A terminal `DEAD_LETTERED` state for events that must not be retried anymore.
- An `EventProcessingFailed` notification event published when handling fails.
- Full backward compatibility: existing deployments, schemas, and mixed-version service fleets keep today's behavior.

## Non-Goals (phase 2 candidates)

- YAML-defined policies per event type (`event-flow.lifecycle.retry.policies.<EventType>`) for ops-tuning without redeploys.
- In-process (in-dispatcher) synchronous retries before falling back to store-based re-publish.
- Blacklist matching (`ignoreRetryOn`).

## Decisions

### D1 — One annotation, `@RetryPolicy`, in two placement modes

Modeled after Spring's `@Transactional`: inline on the listener method, or declared once with a `name` and referenced from `@EventListener`.

```java
// Mode 1: inline on the listener
@EventListener(OrderCreatedEvent.class)
@RetryPolicy(
        retryOn = {TimeoutException.class, SQLException.class}, // empty = any exception
        maxRetries = 5,
        initialDelay = 10, unit = TimeUnit.SECONDS,
        multiplier = 2.0,
        maxDelay = 5                        // same unit
)
public void on(OrderCreatedEvent event) { ... }

// Mode 2: named, reusable policy (declared on a TYPE, scanned at startup)
@RetryPolicy(name = "payment-retry",
        retryOn = PaymentUnavailableException.class,
        maxRetries = 10, initialDelay = 5, unit = TimeUnit.SECONDS)
public class RetryPolicies { }

@EventListener(value = PaymentFailedEvent.class, policy = "payment-retry")
public void on(PaymentFailedEvent event) { ... }
```

Resolution order: **method-level `@RetryPolicy` → named policy referenced by `@EventListener.policy` → global defaults** from `event-flow.publisher.lifecycle.retry.*`. Attributes left unset inherit the global defaults. An unknown policy name is a fail-fast startup error.

`@RetryPolicy` targets `METHOD, TYPE`. The `name` attribute is only legal on `TYPE` (validated at registration); method-level declarations must not set it.

### D2 — Classification happens on the consumer side

The listener's policy lives where the listener lives. `UnifiedEventDispatcher` unwraps the exception chain (`EventHandlerInvocationException` → `InvocationTargetException` → cause chain) and matches it against `retryOn` (whole-cause-chain `isInstance` match). The dispatcher computes, per failed handler:

- `retryable` — whether the exception matches the policy,
- `attempt` — from `Envelope` metadata key `"attempt"` (absent = 1); `EventLifecyclePublisher` stamps `attempt = retry_count + 1` on every re-publish,
- `willRetry = retryable && attempt <= maxRetries`,
- `retryAfter = min(initialDelay × multiplier^(attempt-1), maxDelay)`.

The results travel to the publisher side inside an extended `FailureAck` (`retryable`, `willRetry`, `retryAfterMs`, `policyName`). The publisher side **executes the schedule**; it never introspects consumer-side annotations. This keeps the publisher-side scheduler usable in deployments where the consuming service is a different application.

**Relative, not absolute time:** the ack carries `retryAfterMs` (a duration). `AckHandler` computes `next_retry_at = now + retryAfter` using **its own clock**, so clock skew between services cannot corrupt the schedule.

### D3 — The event store holds schedule *facts*, not policy *configuration*

The policy (annotation attributes) is re-evaluated on **every** attempt against the currently deployed code. The store persists only the resulting decision:

- `retry_count` (existing) — number of re-publishes performed; current attempt = `retry_count + 1`. Incremented on the transition to `NEW` (unchanged mechanics).
- `next_retry_at` (new) — absolute instant when re-publish becomes eligible. Written only when `willRetry = true`. Meaningful only while `status = FAILED`; cleared on every transition out of `FAILED`.
- `retry_policy` (new, optional) — the policy name that made the decision; diagnostics only ("why is this row queued and when is the next attempt").

Storing a snapshot of policy parameters per row was rejected: the re-publish returns to the consumer, which applies the *current* annotation anyway — a stored snapshot would silently diverge from live behavior (e.g. after changing `maxRetries`).

### D4 — New terminal status `DEAD_LETTERED('D')`

`FAILED` remains "the retry queue" (backward compatible). `DEAD_LETTERED` is terminal: set by `AckHandler` when `willRetry = false` (non-retryable exception, or attempt budget exhausted) and read by nobody in the retry path. Resurrection uses the existing `markForRetry(eventId)` API — the manual flag bypasses `next_retry_at` and limits, no new API is required.

### D5 — `EventProcessingFailed` is published per failed attempt

A technical event carrying `originalEventId`, `originalEventType`, `attempt`, `willRetry`, `failedHandlers`, and the exception chain. Published to the **original event's channels** on every failed attempt with the `willRetry` flag: metric collectors see all attempts, alerting reacts to `willRetry = false`. It is *not* persisted and receives no lifecycle acks (same treatment as `LifecycleAckEvent`). See D8 for the loop-protection design.

### D6 — Multi-handler conflict rule: "any retryable wins"

When several listeners handle one event and at least one failed handler is retryable *and* `willRetry`, the event is scheduled for re-publish (status `FAILED` + `next_retry_at`). Only when **all** failures are permanent/exhausted does it become `DEAD_LETTERED`. Re-publish re-runs *all* handlers for the event, so handlers must be idempotent — already a de-facto requirement of the framework, now documented explicitly.

### D7 — Backward compatibility

- Listeners without `@RetryPolicy`: `retryOn` defaults to "any exception" and parameters to the global config → today's behavior.
- Pre-upgrade rows (`FAILED`, `next_retry_at IS NULL`): the scheduler keeps the legacy per-row heuristic (`updatedAt + minAge × 2^retryCount` in Java).
- Mixed fleets: a new publisher-side `AckHandler` receiving a `FailureAck` from an old consumer (no `willRetry`/`retryAfterMs` fields) treats it as `willRetry = true` with the global backoff — old behavior preserved until the consumer upgrades.

### D8 — Loop protection: technical events are terminal in the failure-notification graph

`EventProcessingFailed` (EPF) must not be able to reproduce itself when its own handling fails. The design breaks every possible loop at three **independent structural points** instead of bounding recursion with a hop counter:

1. **Single emission point with a type guard.** EPF is emitted in exactly one place — `EventLifecycleDispatcher`, and only on failures of *non-technical* events. The emission path is guarded by `instanceof SystemEvent` (marker interface implemented by EPF; `LifecycleAckEvent` may extend it later). Dispatching a technical event can never reach the EPF emission code, even if all of its handlers throw.
2. **No persistence.** `EventLifecyclePublisher` skips technical events, so an EPF never gets an `EventStore` row and can never be selected by `EventRetryScheduler` — there is no mechanism anywhere that re-publishes it.
3. **No acks.** `EventLifecycleDispatcher` passes technical events through without ack wrapping (the proven `LifecycleAckEvent` pass-through pattern): a failing EPF listener yields a logged `HandlerResult.failure` and nothing else — no `FailureAck`, no store update, no schedule.

Consequences of the structure:

- Generation is bounded by construction: per original event at most `maxRetries + 1` EPF instances (one per attempt; the last carries `willRetry = false`).
- EPF is constructed **fresh** with an explicit whitelist of correlation fields (`originalEventId`, `originalEventType`, `processId`, `attempt`, `willRetry`, error chain). The original envelope's metadata map is never copied wholesale, so inherited `pubSrv`/`attempt` keys cannot confuse lifecycle filtering.
- Duplicate deliveries (Kafka at-least-once, publish-time `RetryEventPublisher` re-sends after a partial channel failure) are a delivery-semantics concern, not a loop; they are absorbed by idempotent handling and documented as at-least-once.
- Failures of EPF listeners (e.g. a broken DLQ writer) are contained and logged with a distinct marker — they are never re-entified into new events; moving the problem one level up would just recreate it.

**Rejected alternative — hop counter in metadata (`epfDepth`).** A depth counter only *bounds* recursion (the loop still executes N times), costs an extra field on every publication, and requires propagation discipline at every emission. The type-based guard is absolute and cheaper — reject.

**Out of scope — user-authored cross-event loops.** An EPF listener that publishes new business events which themselves fail (producing EPF #2) is a consumer-side flow the framework cannot distinguish from legitimate business reactions. Documented as a consumer responsibility: EPF listeners must not publish events derived from the failed handling unless the resulting chain is bounded by business logic.

**Required regression tests:** (a) dispatching an EPF with throwing handlers emits no further EPF and no acks; (b) EPF is never persisted; (c) EPF never appears in scheduler selection.

## Schema changes

### Target DDL (PostgreSQL is canonical)

```sql
ALTER TABLE event_store ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMP;
ALTER TABLE event_store ADD COLUMN IF NOT EXISTS retry_policy  VARCHAR(255);

CREATE INDEX IF NOT EXISTS idx_event_store_retry ON event_store(status, next_retry_at);
```

The index serves the primary scheduler predicate `status = 'F' AND next_retry_at <= now`; the existing `idx_event_store_status(status, updated_at)` does not cover it. Status code comments extend with `D=DEAD_LETTERED`.

### Migration for existing installations

`SchemaInitializer` only runs `CREATE TABLE IF NOT EXISTS` — it cannot add columns to a live table. Two work items:

1. An `ensureColumn` step (via `information_schema.columns`, dialect-specific fallback for Oracle) when `auto-init-schema = true`; in verify-only mode fail fast with a message pointing to the release notes.
2. Documented `ALTER` snippets per dialect in the release notes, following the 1.3.0 precedent:

```sql
-- PostgreSQL / H2 / MySQL
ALTER TABLE event_store ADD COLUMN next_retry_at TIMESTAMP NULL;
ALTER TABLE event_store ADD COLUMN retry_policy VARCHAR(255) NULL;

-- Oracle
ALTER TABLE event_store ADD (next_retry_at TIMESTAMP NULL, retry_policy VARCHAR2(255) NULL);

-- SQL Server
ALTER TABLE event_store ADD next_retry_at DATETIME2 NULL, retry_policy NVARCHAR(255) NULL;
```

All five dialect DDL scripts, `SchemaInitializer` (incl. `COMMENT ON COLUMN` for PostgreSQL/Oracle), `SqlDialect` statements, `StoredEventMapper`, `InMemoryEventStore`, and the `EventCleanupScheduler` status list (add `DEAD_LETTERED`, retention configurable via the existing cleanup `maxAge`) are updated accordingly.

## State machine and write responsibilities

| Transition | Writer | `status` | `retry_count` | `next_retry_at` |
|---|---|---|---|---|
| first publish | `EventLifecyclePublisher` | NEW → PUBLISHED | 0 | NULL |
| handling failed, `willRetry = true` | `AckHandler` | **FAILED** | N | **now + retryAfter** |
| handling failed, `willRetry = false` | `AckHandler` | **DEAD_LETTERED** | N | NULL |
| re-publish by scheduler | `EventLifecyclePublisher` | NEW | **N+1** | **NULL** (reset) |
| success after retry | `AckHandler` (SuccessAck) | HANDLED | N+1 (kept) | NULL |
| manual `markForRetry` | operator / API | any → retried bypassing schedule | unchanged | cleared on re-publish |

Invariant: `next_retry_at` is populated **iff** `status = FAILED`.

## Scheduler query and per-row logic

```sql
WHERE service = ?
  AND (retry = TRUE                                    -- manual retry (unchanged)
       OR (status = 'F' AND next_retry_at <= now)      -- new path: policy schedule
       OR (status IN ('P','N') AND updated_at <= ?)    -- stuck: no ack / crash before publish
       OR (status = 'F' AND next_retry_at IS NULL      -- legacy rows, pre-upgrade
           AND updated_at <= ?))
```

Per-row in Java:

```java
if (event.nextRetryAt() != null) {
    // New path: the budget was already applied by the consumer via willRetry;
    // only the schedule gate remains.
    if (!Instant.now().isBefore(event.nextRetryAt())) {
        retryEvent(event);
    }
} else {
    // Legacy/stuck path: global maxRetries + minAge × 2^retryCount (today's behavior).
    if (event.retryCount() >= maxRetries) { log.warn(...); return; }
    if (!event.isReadyForRetry(minAge)) return;
    retryEvent(event);
}
```

In the new path the scheduler no longer needs the global `maxRetries` for policy-driven events — exhaustion is decided on the consumer side and the event goes straight to `DEAD_LETTERED` from `AckHandler`. The global limit remains only as a safety net for stuck events whose ack never arrived. Effective wake-up time is `next_retry_at` rounded up to the next scheduler cycle (`retry-interval`, default 30s).

## Component change map

| Component | Change |
|---|---|
| **new** `@RetryPolicy` | `retryOn[]`, `maxRetries`, `initialDelay`, `unit`, `multiplier`, `maxDelay`, `name`; targets `METHOD, TYPE` |
| `@EventListener` | + `policy()` attribute (String, policy name) |
| **new** `RetryPolicyRegistry` | populated at startup by scanning method/type-level annotations (core) + Spring bean scanning; fail-fast on unknown names |
| `EventListenerRegistry` / `MethodInvokingEventHandler` | carry the resolved policy with the handler |
| `UnifiedEventDispatcher` | computes `retryable` per failed handler (cause-chain unwrapping + match) |
| `HandlerResult` | + `retryable` component; legacy 4-arg constructor retained (delegates with `retryable = true`) |
| `FailureAck` | + `retryable`, `willRetry`, `retryAfterMs`, `policyName`; legacy constructor retained |
| **new** `SystemEvent` | marker interface for technical events: no persistence, no acks, no EPF emission (see D8) |
| **new** `EventProcessingFailed` | per-attempt failure notification; `implements SystemEvent`; technical (no persistence, no acks) |
| `EventLifecycleDispatcher` | publishes `EventProcessingFailed` next to `FailureAck`; emission guarded — never for `SystemEvent` instances |
| `AckHandler` | routes FAILED + `next_retry_at` vs `DEAD_LETTERED` based on `willRetry` |
| `EventStore` | `updateStatus(..., nextRetryAt, ...)` overload; `findRetryableEvents` includes `next_retry_at` predicate |
| `StoredEvent` | + `nextRetryAt`, `retryPolicy`; `withRetrySchedule(...)`; `withRetry()` clears the schedule |
| `JdbcEventStore` / `InMemoryEventStore` / `StoredEventMapper` | new columns / fields |
| 5 SQL dialects + DDL scripts + `SchemaInitializer` | `next_retry_at`, `retry_policy`, status `D`, index, `ensureColumn` |
| `EventRetryScheduler` | dual-path eligibility (schedule vs legacy heuristic) |
| `EventLifecyclePublisher` | stamps `attempt` into `Envelope` metadata on re-publish |
| `EventFlowProperties` | global defaults for policy attribute inheritance |
| `EventCleanupScheduler` | include `DEAD_LETTERED` in purge statuses (retention = existing `maxAge`) |

`IdempotentEventDispatcher` and `RetryEventPublisher` need no changes: the idempotency cache is invalidated on failure, so a re-published event with the same `eventId` is reprocessed correctly; publish-time retry operates at a different layer.

## Consequences

### Positive

- Failure handling becomes declarative and per-listener; poison messages stop clogging the retry queue.
- Terminal failures are first-class (`DEAD_LETTERED` + `EventProcessingFailed`), enabling reactive alerting/DLQ sinks.
- The schedule is computed where the policy lives and executed where the store lives — clean consumer/publisher separation, safe under clock skew and service splits.
- Backward compatible by construction (legacy rows, missing ack fields, listeners without policies).

### Negative / trade-offs

- Schema migration required (two columns + one index); `SchemaInitializer` grows an `ensureColumn` responsibility.
- `HandlerResult` and `FailureAck` records gain components — mitigated by retained legacy constructors, but the canonical constructors change (noted in release notes).
- Re-publish re-runs all handlers; per-handler retry isolation is out of scope (documented idempotency requirement).
- Effective retry latency is bounded below by the scheduler interval (`retry-interval`).

## Open questions

| # | Question | Recommendation |
|---|---|---|
| 1 | YAML per-event-type policies in MVP? | No — phase 2; annotation + named policies cover the request |
| 2 | `ignoreRetryOn` blacklist in MVP? | No — phase 2; whitelist (`retryOn`, empty = all) is backward compatible |
| 3 | `EventProcessingFailed` per attempt vs terminal only? | Per attempt with `willRetry` (D5) — flexible for both metrics and alerting |
| 4 | Include diagnostic `retry_policy` column in MVP? | Yes — one nullable VARCHAR, high diagnostic value for a stuck-row triage |
| 5 | In-process fast retries before store round-trip? | No — phase 2 (non-goal above) |

## Implementation phases

**Phase 1 (this ADR):** `@RetryPolicy` API + registry, dispatcher classification, extended `FailureAck`/`HandlerResult`, `EventProcessingFailed`, `AckHandler` routing, `next_retry_at`/`retry_policy` columns, `DEAD_LETTERED` status, dual-path scheduler, migration support, tests, README/CHANGELOG updates.

**Phase 2 (candidates):** YAML per-event-type policies, `ignoreRetryOn`, in-dispatcher synchronous retries.
