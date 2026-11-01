# ADR-0002: Two-Layer Listener Retry — Reconsideration of ADR-0001

- **Status:** Superseded by [ADR-0004](0004-two-domain-event-retry.md) (Proposed) — retained as historical context
- **Date:** 2026-10-18
- **Author:** Vladimir Aleshkov (ADR-0001 author, reconsidered with architecture review)
- **Relates to:** [ADR-0001](0001-listener-retry-policies.md) — superseded its Decisions D2–D8; D1 was retained with a narrower (local-only) scope. Its local per-listener retry idea is retained by ADR-0004.
- **Tags:** retry, dead-letter, lifecycle, in-process, event-store, api

## Context

### ADR-0001 in one paragraph

ADR-0001 proposed per-listener retry policies declared with `@RetryPolicy`, classified **on the consumer side**, with the resulting schedule (`willRetry`, `retryAfterMs`, `policyName`) transported back to the publisher inside an extended `FailureAck`. The publisher persisted the schedule as facts (`next_retry_at`, `retry_policy` columns), executed it in `EventRetryScheduler`, added a terminal `DEAD_LETTERED` status, and published a `EventProcessingFailed` notification per failed attempt. A new `SystemEvent` marker guarded against notification loops.

### Why reconsider

ADR-0001 is internally consistent and well-reasoned, but it pays seven structural costs to serve one narrow goal:

| # | Structural cost of ADR-0001 |
|---|---|
| 1 | **Re-publish re-runs every handler.** One failing listener forces all other listeners of the event through the channel again — idempotency becomes a hard requirement for every listener, and one slow/failing listener drags all others along on every attempt. |
| 2 | **The schedule travels in an ack.** `FailureAck` must carry `willRetry`/`retryAfterMs`/`policyName`; the envelope must carry `attempt`; the publisher must guard against clock skew and against mixed fleets ("old consumer without `willRetry` degrades to global backoff"). |
| 3 | **Two technical events per failed attempt.** `FailureAck` (already enriched with `willRetry`) and `EventProcessingFailed` (with `failedHandlers` + exception chain) are published from the same place with almost the same content. |
| 4 | **A new `SystemEvent` marker** that duplicates the loop-safety the framework already has in `LifecycleAckEvent`. |
| 5 | **No publisher-side hard cap.** Once the consumer decides `willRetry`, the global `maxRetries` no longer applies — a `willRetry` bug or an oversized policy loops an event forever (a re-publish DDoS). |
| 6 | **No jitter.** Events with identical schedules become eligible in lock-step — a thundering herd on every re-publish wave. |
| 7 | **`DEAD_LETTERED` cleanup vs manual resurrection.** Purging dead letters by the shared `maxAge` deletes the very rows `markForRetry(eventId)` needs to resurrect. |

### The unstated assumption that drives the whole design

ADR-0001 implicitly requires **durable per-listener retry schedules** — the exact backoff and budget of a specific listener must survive a process crash and continue from the store. That single requirement forces all of the above machinery.

This ADR starts from the opposite premise: **per-listener retry exists to absorb transient failures cheaply, where the listener lives.** Crash recovery is already provided by the existing durable event-level retry. Once that premise is accepted, most of ADR-0001's machinery disappears.

## Goals (revised)

- Exception classification declared per listener (`@RetryPolicy`), executed **locally in the dispatcher** for transient failures — fast, no ack round-trip, no store.
- Durable recovery for events that exhaust local retries stays **event-level and fully backward compatible** (existing scheduler, existing schema).
- A terminal `DEAD_LETTERED` state, decided on the **publisher side**, where `retry_count` and the global budget already live.
- One loop-safe failure notification event, subscribable for metrics/alerting/DLQ sinks.
- **No ack protocol extension that transports schedules, no new columns, no clock-skew handling, no `attempt` stamping.**

## Non-Goals (phase 2 candidates)

- YAML per-event-type policies — now trivially additive (see D2).
- Per-handler durable isolation (`PARTIALLY_HANDLED`) — see D7.
- `ignoreRetryOn` blacklist.
- A `DeadLetteredEvent` carrying the payload to a DLQ channel — see D6.

## Decisions

### D1 — Two-layer retry model (replaces ADR-0001 D2–D4, D7)

**Layer 1 — local, in-dispatcher, per-listener.** The resolved `@RetryPolicy` is carried with each handler. Inside `UnifiedEventDispatcher.executeHandler`, the handler invocation is wrapped in a retry loop:

```java
private HandlerResult executeHandler(EventHandler handler, Event event) {
    RetryPolicy policy = handler.retryPolicy();          // resolved at registration; empty = no policy
    int attempt = 1;
    while (true) {
        try {
            handler.onEvent(event);
            return HandlerResult.success(handler.name());
        } catch (Exception e) {
            if (policy == null)                          // no policy → today's behavior, never "permanent"
                return HandlerResult.failure(handler.name(), e);
            if (!policy.matches(e))                      // not in retryOn → permanent
                return HandlerResult.failure(handler.name(), e).permanent();
            if (attempt > policy.maxRetries())           // budget exhausted → transient-exhausted
                return HandlerResult.failure(handler.name(), e);
            sleep(policy.backoff(attempt));              // initialDelay × multiplier^(attempt-1), capped
            attempt++;
        }
    }
}
```

- Only the failing handler is retried, in place — **other listeners are not re-run** (removes cost #1 for the transient case).
- Backoff runs on the dispatcher executor thread: free with virtual threads, consumes a pool thread with platform threads (see Consequences).
- Failure classification: **permanent** = a policy exists and the exception does not match `retryOn`; **transient-exhausted** = matches `retryOn` but the local budget ran out; **unclassified** = no policy.

**Layer 2 — durable, store-based, event-level (mostly unchanged).** When any handler still failed after Layer 1, the existing `FailureAck` → `FAILED` → `EventRetryScheduler` path applies, with the existing global `minAge × 2^retryCount` backoff and global `maxRetries`. The only change is the exhaustion endpoint (D4).

| Concern | ADR-0001 | This ADR |
|---|---|---|
| Per-listener backoff/budget | consumer computes → ack → store → scheduler | **in the dispatcher** (Layer 1) |
| Crash-survivable per-listener schedule | yes | no (durable path is event-level) |
| Re-run all handlers on retry | yes, every attempt | no (Layer 1); yes only after durable re-publish |
| Ack extension | schedule fields + attempt stamping | one boolean `permanent` (+ `failedHandlers`) |
| New columns / index | `next_retry_at`, `retry_policy`, index | **none** (only status `D`) |
| Mixed-fleet behaviour | old consumer degrades | old consumer unchanged (`permanent` defaults false) |

### D2 — `@RetryPolicy` is a local, consumer-side concern (retains ADR-0001 D1)

The annotation, named-policy registry, resolution order (method → `@EventListener.policy` → global defaults) and fail-fast on unknown names are kept **exactly as in ADR-0001 D1** — but resolution now feeds only the local Layer-1 loop. Nothing crosses the wire.

- Listeners **without** `@RetryPolicy` keep today's behaviour: no local retry, failure → `FailureAck` → global retry (unclassified, never `permanent`).
- YAML per-event-type policies (phase 2) become a drop-in: they would only need to populate the same resolved-policy slot on the consumer side; nothing in the protocol changes.

### D3 — One failure signal: enriched `FailureAck`; no `EventProcessingFailed`, no `SystemEvent`

`FailureAck` already `implements LifecycleAckEvent`. That one fact already gives us **all three loop guards of ADR-0001 D8 for free**:

1. `EventLifecycleDispatcher.dispatch()` passes `LifecycleAckEvent` through unwrapped → a failing `FailureAck` listener produces no acks and no further failure events (guards 1 and 3).
2. `EventLifecyclePublisher.shouldSkipPersistence()` skips `LifecycleAckEvent` → a `FailureAck` is never persisted and never selected by the scheduler (guard 2).

So the notification requirement ("an event when processing fails", subscribable by metrics/alerting/DLQ) is satisfied by `FailureAck` itself. We enrich it minimally:

```java
public record FailureAck(
        UUID eventId, UUID originalEventId, String eventType, String originalService,
        String error,
        List<String> failedHandlers,      // + names of handlers that failed
        boolean permanent,                // + true iff every failed handler was classified permanent
        List<Class<? extends EventChannel>> sourceChannels,
        UUID processId, Instant occurredAt
) implements LifecycleAckEvent, TraceableEvent { ... }
```

`permanent` is the **only** value the publisher needs: `true` → dead-letter now; `false`/absent → `FAILED` → global retry. `failedHandlers` is diagnostics. No `EventProcessingFailed` event, no `SystemEvent` marker, no dispatcher/publisher loop-logic changes.

Rejected: keeping both `FailureAck` and `EventProcessingFailed` as separate public contracts. The protocol/stability argument for two events is real but outweighed by a single loop surface, half the bus traffic, and the reuse of a proven mechanism.

### D4 — Publisher-side exhaustion and a hard safety cap

Exhaustion is decided where the evidence lives — on the publisher side:

- `AckHandler` on `FailureAck.permanent = true` → `DEAD_LETTERED` immediately (no wasted global re-publishes, other handlers are never re-run for a hopeless event).
- `EventRetryScheduler` on `retry_count >= global maxRetries` for a still-`FAILED` event → `DEAD_LETTERED` **instead of today's `log.warn` + skip**.
- The global `maxRetries` remains an **absolute ceiling that nothing can bypass** — even if the consumer keeps classifying failures as transient, the event terminates at the global budget. This closes the ADR-0001 infinite-loop hole (#5).

### D5 — Jitter on the durable path

The durable path inherits the existing lock-step backoff (`minAge × 2^retryCount` per row). Add a small random jitter (±15%, or full jitter `[0, backoff)`) to each row's readiness gate to avoid re-publish herds when many events fail in a wave. No schema impact — applied in Java in `EventRetryScheduler`.

### D6 — `DEAD_LETTERED` retention and actionable dead letters

- `DEAD_LETTERED` gets its **own retention**, separate from `HANDLED`/`UNDEFINED`, so dead letters survive long enough for manual triage and `markForRetry(eventId)` resurrection.
- Phase 2: a `DeadLetteredEvent` carrying the **full payload** published to a DLQ channel, so reprocessing/archiving no longer depends on the store row surviving.

### D7 — Explicit partial-success decision (open question, decided here provisionally)

One permanently-failing handler currently poisons the event for everyone: it never becomes `HANDLED`, burns the global budget, and is dead-lettered as a whole. This ADR accepts that for now (consistent, bounded) but records it as the explicit product decision, with `PARTIALLY_HANDLED` (event counts as handled if at least one handler succeeded and the rest failed permanently/exhausted, with per-handler DLQ) as the phase-2 resolution. See Open questions.

## Schema changes

Exactly one change: the status code `D = DEAD_LETTERED`.

```sql
-- no DDL required for the retry redesign itself.
-- EventStatus gains 'D'; comment on column status extended.
```

No `next_retry_at`, no `retry_policy`, no index. `InMemoryEventStore`, `StoredEventMapper`, all five dialects and `SchemaInitializer` change only in that `EventStatus` values, `COMMENT ON COLUMN`, and the `EventCleanupScheduler` status list are extended.

## State machine and write responsibilities

| Transition | Writer | `status` | `retry_count` |
|---|---|---|---|
| first publish | `EventLifecyclePublisher` | NEW → PUBLISHED | 0 |
| handling failed, no `permanent` (`permanent = false` / unclassified) | `AckHandler` (FailureAck) | **FAILED** | N |
| handling failed, `permanent = true` | `AckHandler` (FailureAck) | **DEAD_LETTERED** | N |
| re-publish by scheduler (global budget left) | `EventLifecyclePublisher` | NEW | **N+1** |
| success after retry | `AckHandler` (SuccessAck) | HANDLED | N+1 (kept) |
| global budget exhausted, still FAILED | `EventRetryScheduler` | **DEAD_LETTERED** | N |
| manual `markForRetry` | operator / API | any → retried (bypasses budget) | unchanged |

Invariant: no new column to keep consistent — `DEAD_LETTERED` is terminal and read by nobody in the retry path.

## Component change map

| Component | Change |
|---|---|
| **new** `@RetryPolicy` | `retryOn[]`, `maxRetries`, `initialDelay`, `unit`, `multiplier`, `maxDelay`, `name` (as ADR-0001 D1) |
| `@EventListener` | + `policy()` attribute (unchanged from ADR-0001) |
| **new** `RetryPolicyRegistry` | startup scan of method/type annotations; fail-fast on unknown names |
| `EventListenerRegistry` / `MethodInvokingEventHandler` | carry the resolved policy with the handler |
| `UnifiedEventDispatcher` | Layer-1 retry loop + permanent classification inside `executeHandler` |
| `HandlerResult` | + `permanent` flag (default false); existing factories retained |
| `FailureAck` | + `failedHandlers`, `permanent`; legacy constructor retained (`permanent = false`) |
| `AckHandler` | route `permanent` → `DEAD_LETTERED`, else `FAILED` |
| `EventRetryScheduler` | on exhaustion `FAILED → DEAD_LETTERED`; jitter on readiness gate |
| `EventStore` / `StoredEvent` / stores | no interface change (only status `D`; `markForRetry` reuse) |
| `EventLifecycleDispatcher` / `EventLifecyclePublisher` | **no change** — loop safety already via `LifecycleAckEvent` |
| `EventFlowProperties` | global defaults for policy attribute inheritance |
| `EventCleanupScheduler` | include `DEAD_LETTERED` in purge, with a separate (longer) retention |

`IdempotentEventDispatcher` and `RetryEventPublisher` need no changes.

## Full cycle example

Scenario — payment service, `InternalEventChannel`, `MANAGED` lifecycle:

```java
@Event(channels = InternalEventChannel.class, lifecycle = EventLifecycle.MANAGED)
public record OrderCreated(String orderId, String customerId, long amount) {}

// Listener A — transient TimeoutException from the payment gateway
@EventListener(OrderCreated.class)
@RetryPolicy(retryOn = TimeoutException.class,
        maxRetries = 3, initialDelay = 100, unit = TimeUnit.MILLISECONDS, multiplier = 2.0)
public void charge(OrderCreated e) { gateway.charge(e); }   // throws TimeoutException | PaymentRejectedException

// Listener B — audit, always succeeds
@EventListener(OrderCreated.class)
public void audit(OrderCreated e) { auditLog.append(e); }
```

**Cycle A — transient failure absorbed locally (attempt 1):**

```
publish ─► store: NEW ─► channel ─► dispatcher
                                          │
                PaymentProcessor ─ retry loop (Layer 1)
                    attempt 1: TimeoutException → sleep 100ms
                    attempt 2: TimeoutException → sleep 200ms
                    attempt 3: ok
                AuditLogger  ─ runs exactly once (never re-run)
                                          │
                     SuccessAck ─► AckHandler ─► store: HANDLED
```

Result: delivered and handled once; `AuditLogger` ran once; no store round-trip, no ack delay, no clock skew. This is the common case and it costs almost nothing.

**Cycle B — local budget exhausted → durable global retry → success (attempts 2–3):**

```
attempt 2:
    PaymentProcessor: TimeoutException ×3 (100/200/400ms) → transient-exhausted
    AuditLogger: ok
    FailureAck(permanent = false) ─► AckHandler ─► store: FAILED

    EventRetryScheduler (backoff minAge×2^0) ─► re-publish ─► store: NEW (retry_count=1)
attempt 3:
    PaymentProcessor: ok on first local try
    AuditLogger: ok
    SuccessAck ─► store: HANDLED
```

Note: after the durable re-publish, **all** listeners are re-run once (this is the unavoidable re-publish path); `AuditLogger` therefore ran once per delivery — idempotency is required exactly here, not for local retries.

**Cycle C — permanent failure → immediate dead-letter (attempt 1):**

```
attempt 1:
    PaymentProcessor: PaymentRejectedException (card declined)
        → NOT in retryOn → no local retries → permanent
    AuditLogger: ok
    FailureAck(permanent = true) ─► AckHandler ─► store: DEAD_LETTERED
```

No wasted global re-publishes, `AuditLogger` never re-runs. Metrics/alerting subscribe to `FailureAck` and see `permanent = true`; an operator fixes the data and calls `markForRetry(eventId)` → scheduler re-publishes → retried.

**Cycle D — poison event (transient that never recovers) → global budget → dead-letter:**

```
TimeoutException keeps returning on every delivery:
    each delivery: local ×3 → FailureAck(permanent=false) → FAILED → re-publish
    ... until retry_count >= global maxRetries (3) ...
EventRetryScheduler: FAILED → DEAD_LETTERED   (instead of today's log.warn + skip)
```

The global cap is the absolute ceiling — even a consumer misclassification cannot loop forever.

## Consequences

### Positive

- The common case (transient failure) is handled locally in milliseconds without touching the store, the ack channel, or other listeners.
- Schema change shrinks from two columns + an index to a single status code; no ack schedule protocol, no clock-skew handling, no `attempt` stamping.
- Loop safety reuses the proven `LifecycleAckEvent` mechanism instead of a new marker.
- Exhaustion and the hard budget live on the publisher side where `retry_count` is authoritative — no consumer can drive unbounded re-publishes.
- Backward compatible by construction: no policy → today's behaviour; old consumer → `permanent` absent = false; no schema migration.

### Negative / trade-offs

- **Crash-survivable per-listener schedules are explicitly out**: a retry in progress is lost on crash; the event is then re-delivered by the durable event-level path (which is at-least-once and requires idempotency anyway). This is the deliberate trade for removing ADR-0001's machinery.
- Blocking Layer-1 backoff occupies the dispatcher executor thread during sleep: with virtual threads this is free, with a small platform pool it reduces effective concurrency (mitigate by a modest `maxRetries`/`maxDelay` or a dedicated retry executor).
- One permanently-failing handler still dead-letters the event as a whole (`PARTIALLY_HANDLED` deferred, D7).
- Durable retry latency is still bounded below by the scheduler interval — but for the common transient case the local layer returns milliseconds, so the scheduler interval no longer dominates.

## Open questions

| # | Question | Recommendation |
|---|---|---|
| 1 | Per-handler durable isolation (`PARTIALLY_HANDLED`): event counts as handled if ≥1 handler succeeded and the rest failed permanently? | Phase 2 — requires per-handler result tracking in the store; documented in D7 |
| 2 | YAML per-event-type policies now or later? | Phase 2 — becomes trivial (populates the same resolved-policy slot, D2) |
| 3 | `DeadLetteredEvent` with payload to a DLQ channel? | Phase 2 (D6) — base design notifies via `FailureAck`, which carries no payload |
| 4 | `ignoreRetryOn` blacklist? | Phase 2 |
| 5 | Blocking backoff on platform-thread pools acceptable? | Yes with documented limits; dedicated retry executor as phase-2 refinement |

## Implementation phases

**Phase 1 (this ADR):** `@RetryPolicy` + registry, Layer-1 retry loop in `UnifiedEventDispatcher`, `HandlerResult.permanent`, enriched `FailureAck`, `AckHandler` routing, `DEAD_LETTERED` status, scheduler exhaustion transition + jitter, `EventCleanupScheduler` retention split, tests, README/CHANGELOG updates.

**Phase 2 (candidates):** YAML policies, `DeadLetteredEvent` + DLQ channel, `PARTIALLY_HANDLED` / per-handler durable isolation, `ignoreRetryOn`, dedicated retry executor.
