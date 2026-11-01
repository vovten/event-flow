# ADR-0004: Two-Domain Event Retry — Delivery and Processing

- **Status:** Proposed
- **Date:** 2026-11-01
- **Author:** project maintainer (design review)
- **Relates to:**
  - supersedes [ADR-0001](0001-listener-retry-policies.md);
  - supersedes [ADR-0002](0002-listener-retry-two-layer.md), retaining its local per-listener retry idea in a revised form;
  - supersedes [ADR-0003](0003-retry-delivery-processing.md), adopting its delivery/processing boundary but simplifying storage and MVP scope.
- **Implementation plan:** [ADR-0004 Implementation Plan](../plans/adr-0004-implementation-plan.md)
- **Tags:** retry, delivery, processing, lifecycle, dead-letter, event-store, ack, api

## Context

### The current ambiguity

The lifecycle currently has one failure status, `FAILED`, and one retry scheduler that treats `FAILED`, `PUBLISHED` and `NEW` as retry candidates. This conflates two different questions:

1. **Was the event delivered to a consumer?**
2. **Did the consumer's handler process it successfully?**

A `publish()` exception does not answer the first question. The broker may already have accepted and delivered the event; the exception may be a lost acknowledgement, a timeout after a successful send, or a partial-channel failure. Treating every `publish()` exception as "not delivered" causes unnecessary duplicate deliveries.

Conversely, a missing final ack does not prove that processing failed. It may mean the event was never delivered, the consumer crashed before handling it, or the ack itself was lost.

The existing acks (`SuccessAck` / `FailureAck`) are emitted only after handler execution. They answer the processing question but not the delivery question. There is no consumer-side acceptance signal.

### Why this matters in the current implementation

- `KafkaInTransport` auto-commits offsets and does not wait for `dispatcher.dispatch()` to complete. A crash after receipt but before handler completion is not redelivered by Kafka, so a durable processing-retry path is required.
- `EventLifecyclePublisher` persists before publish and marks `PUBLISHED` when the transport call succeeds. That is broker acceptance, not consumer acceptance.
- `EventRetryScheduler` re-publishes the whole event and re-runs every handler. This is acceptable only as a durable fallback; it must not be the first line of defence for transient handler failures.
- `IdempotentEventDispatcher` currently invalidates its cache only on `isAllFailure()`. A partial handler failure leaves the event cached, so a later processing retry is incorrectly dropped as a duplicate.

### ADR history in one paragraph

ADR-0001 proposed per-listener policies with the schedule transported back to the publisher in `FailureAck`, persisting `next_retry_at` / `retry_policy` in `event_store`, a new `EventProcessingFailed` event and a `SystemEvent` loop guard. It paid too much structural cost for one narrow goal.

ADR-0002 replaced it with a two-layer model: local in-dispatcher retries plus the existing durable event-level path. It correctly introduced local per-listener retries and a publisher-side hard cap, but its durable layer still conflates delivery and processing, and it defers multi-handler isolation.

ADR-0003 identified the missing delivery/processing boundary and proposed `AcceptedAck`, an `event_retry` table with `DELIVERY` / `PROCESSING` kinds, leases and a transactional `LifecycleRepository`. This is the right direction, but its full scope (two tables, policy table, several open questions) is heavier than needed for an MVP.

This ADR consolidates the three: it keeps the local per-listener retry from ADR-0002 and the delivery/processing boundary from ADR-0003, while reducing durable storage to a single retry table and keeping policy storage out of the database for the first phase.

## Goals

- Define delivery success by an explicit consumer-side acceptance signal, not by the absence of a `publish()` exception.
- Separate delivery retries from processing retries in state, scheduling, configuration, metrics and failure handling.
- Retry transient handler failures locally, per listener, without touching the event store.
- Provide durable recovery for events that exhaust local retries or whose process crashes.
- Keep the existing `EventStatus` model coarse; `FAILED` remains a common failure marker.
- Store retry details in a dedicated table, not in `EventStatus` or in a growing set of `event_store` columns.
- Allow per-listener processing policies in code with configuration overrides, without a database policy table in phase 1.
- Enforce absolute publisher-side caps so no consumer classification or bug can cause unbounded re-publishes.
- Preserve at-least-once semantics; exactly-once delivery and processing remain out of scope.
- Preserve backward compatibility for listeners without policies, existing `FAILED` rows and mixed-version fleets.

## Non-Goals (phase 2+ candidates)

- Exactly-once processing.
- Replacing the existing event store.
- A database-backed `event_retry_policy` table and seeding from code.
- Per-handler durable isolation and a `PARTIALLY_HANDLED` status.
- Per-destination delivery retry rows (retry only the failed channel).
- A `DeadLetteredEvent` carrying the full payload to a DLQ channel.
- `EventProcessingFailed` and a new `SystemEvent` marker — the existing `LifecycleAckEvent` mechanism is sufficient.
- Transporting the retry schedule from consumer to publisher in an ack.
- YAML-defined per-event-type policies in the MVP; named policies and configuration overrides cover the requirement.

## Decisions

### D1 — Two independent retry domains

The retry model has two domains plus one permanent configuration failure:

| Domain | Question | Success evidence | Failure handling |
|---|---|---|---|
| `DELIVERY` | Did the consumer receive the event? | `AcceptedAck` | delivery retry |
| `PROCESSING` | Did the handler process it? | `SuccessAck` | processing retry |
| `NO_HANDLER` | Was a listener found? | — | permanent failure, dead-letter, no automatic retry |

Rules:

- A `publish()` exception is **delivery uncertainty**, not proof of non-delivery.
- If `AcceptedAck` arrives, delivery is confirmed and no delivery retry is needed.
- If `AcceptedAck` does not arrive within the delivery timeout, the event is retried as a delivery failure.
- After `AcceptedAck`, a missing final ack is a **processing uncertainty**, and a processing retry is scheduled after the processing timeout.
- A missing handler is not transient. Retrying it only produces the same result; it goes to `DEAD` and is visible to operators.
- Delivery and processing attempts are counted and scheduled separately.

### D2 — Ack protocol: `AcceptedAck` is the delivery/processing boundary

`EventLifecycleDispatcher` publishes `AcceptedAck` **before** invoking handlers for `MANAGED`, `TraceableEvent`, non-technical events.

```text
Publisher                         Dispatcher
    |                                  |
    |-- publish ---------------------->|
    |                                  | received
    |<---------- AcceptedAck ----------|   (before handler execution)
    |                                  | resolve + execute handlers
    |<---------- SuccessAck -----------|
    |<---------- FailureAck -----------|
    |<---------- NoHandlerAck ---------|   (no listener resolved)
```

Acks and ordering:

- `AcceptedAck` confirms receipt and is emitted before handler resolution. `NoHandlerAck` reports that no listener was resolved. Both may be emitted for the same event; `NoHandlerAck` moves the event to the terminal `DEAD` state, and a late `AcceptedAck` is ignored.
- `AcceptedAck` confirms **delivery only**. It does not mean the handler succeeded.
- `SuccessAck` / `FailureAck` close the processing domain.
- If `AcceptedAck` is lost, a delivery retry may produce a duplicate. This is accepted at-least-once behaviour.
- If the final ack is lost, a processing retry may produce a duplicate. This is also accepted at-least-once behaviour.
- All acks `implement LifecycleAckEvent`: they are not persisted and get no lifecycle acks, so no additional loop guard is required.
- **`retryKind` is the dispatch context, not the ack domain.** In `SuccessAck` / `FailureAck`, `retryKind` identifies which retry row produced the current dispatch: `DELIVERY` when the event was re-published by the delivery scheduler, `PROCESSING` when re-published by the processing scheduler. `attempt` is the 1-based attempt within that context. The publisher stamps both into the Envelope metadata; the consumer echoes them. A first publish has no retry context and defaults to `PROCESSING` / `attempt = 1`. `AcceptedAck` is always delivery and carries only the delivery attempt.
- Acks carry `retryKind` and `attempt` so a stale ack from an older attempt cannot overwrite a newer attempt.
- **Ack order is not guaranteed.** A `SuccessAck` may arrive before `AcceptedAck`. Processing acks (`SuccessAck`, `FailureAck`, `NoHandlerAck`) also prove delivery and must clear the `DELIVERY` row. A late `AcceptedAck` must never roll back `HANDLED`, `DEAD`, or a `FAILED` processing state; from `FAILED` it only clears the delivery row and leaves the status unchanged.
- `AcceptedAck` carries `maxProcessingMs` — the estimated maximum local processing time (including local backoff). The publisher uses it to size the processing timeout and avoid retrying while the consumer is still legitimately retrying locally.

Proposed contracts (fields shown for review; legacy constructors retained):

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
) implements LifecycleAckEvent, TraceableEvent { ... }

public record NoHandlerAck(
        UUID eventId,
        UUID originalEventId,
        String eventType,
        String originalService,
        int attempt,
        List<Class<? extends EventChannel>> sourceChannels,
        Instant occurredAt
) implements LifecycleAckEvent, TraceableEvent { ... }
```

`FailureAck` gains `retryKind`, `attempt`, `retryable`, `failedHandlers` and `errorType`. The legacy constructor defaults to `retryKind = PROCESSING`, `retryable = true` and `attempt = 1`, preserving old behaviour. `SuccessAck` gains `retryKind` and `attempt`, with a retained legacy constructor.

Emitter placement:

- `EventLifecycleDispatcher.dispatch()` publishes `AcceptedAck` immediately after the `LifecycleAckEvent` pass-through check and before `origin.dispatch()`.
- After dispatch it publishes `NoHandlerAck` when `HandlerResults` is empty and not a duplicate, otherwise `FailureAck` or `SuccessAck` as today.
- A duplicate dispatched by `IdempotentEventDispatcher` is treated as an already-processed delivery: emit `SuccessAck` (current behaviour) and never `NoHandlerAck`.

### D3 — Local per-listener processing retry (consumer side)

Per-listener retry is a local, consumer-side concern. It absorbs transient failures quickly and keeps the flexible policy where the listener lives. Nothing crosses the wire.

```java
@EventListener(OrderCreated.class)
@RetryPolicy(
        maxRetries = 3,
        initialDelay = "100ms",
        multiplier = 2.0,
        maxDelay = "5s",
        jitter = 0.5,
        retryOn = { TimeoutException.class },          // empty = any exception
        noRetryOn = { PaymentRejectedException.class } // wins over retryOn
)
public void on(OrderCreated event) { ... }
```

Named, reusable policy:

```java
@RetryPolicy(name = "payment", maxRetries = 5, initialDelay = "1s", ...)
public class PaymentRetryPolicies { }

@EventListener(value = PaymentFailedEvent.class, retryPolicy = "payment")
public void on(PaymentFailedEvent event) { ... }
```

Resolution order:

1. method-level `@RetryPolicy`;
2. named policy referenced by `@EventListener.retryPolicy`;
3. global processing defaults from configuration.

An unknown policy name is a fail-fast startup error. The `name` attribute is only legal on `TYPE`; method-level declarations must not set it. `@RetryPolicy` targets `METHOD, TYPE`.

Policy semantics:

- Listeners **without** a policy keep today's behaviour: no local retry; a failure goes to durable processing retry.
- `retryOn` empty means "any exception"; `noRetryOn` is a blacklist and wins over `retryOn`.
- Classification per failed handler:
  - `permanent` — a policy exists and the exception does not match `retryOn`, or matches `noRetryOn`;
  - `transient-exhausted` — matches `retryOn` but the local budget is exhausted;
  - `unclassified` — no policy; treated as retryable at the durable layer for backward compatibility.
- Inline policy is authoritative. A named policy may be overridden by configuration:

```properties
event-flow.retry.processing.policies.payment.max-retries=10
event-flow.retry.processing.policies.payment.initial-delay=2s
event-flow.retry.processing.policies.payment.multiplier=2.0
event-flow.retry.processing.policies.payment.max-delay=30s
event-flow.retry.processing.policies.payment.retry-on[0]=io.example.PaymentUnavailableException
event-flow.retry.processing.policies.payment.no-retry-on[0]=io.example.PaymentRejectedException
```

Implementation notes:

- The retry loop is asynchronous. Each attempt is submitted to the dispatcher executor; the delay is scheduled through a scheduler/delayed executor. Do not block a platform-thread worker for the whole backoff.
- The concurrency semaphore is acquired per attempt, not around the whole retry loop.
- Backoff: `min(initialDelay × multiplier^(attempt-1), maxDelay)`, then full jitter `[0, delay]`.
- The local loop is bounded by a global safety cap in addition to the policy.
- `HandlerResult` gains `retryable`, `failureKind`, `attempts` and `policyName`; `HandlerResults` gains aggregate predicates (`hasRetryableFailure`, `hasOnlyPermanentFailures`).
- Multi-handler rule for the durable layer: **any retryable wins**. The event is scheduled for processing retry while at least one failed handler is retryable. Only when all failures are permanent/exhausted does it become `DEAD`.
- Durable processing retry re-runs all handlers of the event. Handlers must therefore be idempotent. Per-handler durable isolation is deferred (D12).

### D4 — One durable retry table, two kinds

A single `event_retry` table is the source of truth for pending retries. It is keyed by `(event_id, retry_kind)` so delivery and processing can be pending for the same event at the same time.

Why not columns on `event_store`:

- delivery and processing may coexist for one event (partial channel failure plus a handler failure);
- one row can hold only one kind and one schedule;
- `event_store` would grow into a wide mix of lifecycle and retry concerns.

Why not two tables:

- one scheduler query and one store abstraction;
- the kind discriminator is a single, well-understood dimension;
- a future per-target key can extend the primary key without introducing a second queue.

Target DDL (PostgreSQL is canonical):

```sql
CREATE TABLE IF NOT EXISTS event_retry (
    event_id         UUID         NOT NULL,
    service          VARCHAR(255) NOT NULL,
    retry_kind       VARCHAR(16)  NOT NULL,   -- DELIVERY | PROCESSING
    attempt          INT          NOT NULL DEFAULT 1,
    next_attempt_at  TIMESTAMP    NOT NULL,
    state            VARCHAR(16)  NOT NULL,   -- PENDING | IN_PROGRESS | DEAD
    reason           VARCHAR(64)  NOT NULL,   -- PUBLISH_FAILED | PARTIAL_PUBLISH_FAILED
                                              -- | ACK_TIMEOUT | HANDLER_FAILED
                                              -- | PROCESSING_TIMEOUT | NO_HANDLER
                                              -- | CAP_EXCEEDED
    last_error_type  VARCHAR(512),
    last_error       TEXT,
    policy_name      VARCHAR(255),
    claimed_by       VARCHAR(255),
    claimed_until    TIMESTAMP,
    created_at       TIMESTAMP    NOT NULL,
    updated_at       TIMESTAMP    NOT NULL,
    PRIMARY KEY (event_id, retry_kind)
);

CREATE INDEX IF NOT EXISTS idx_event_retry_due
    ON event_retry (retry_kind, state, next_attempt_at);

CREATE INDEX IF NOT EXISTS idx_event_retry_service_due
    ON event_retry (service, retry_kind, state, next_attempt_at);
```

Invariant: **if a retry is pending, `event_retry` has a row; if no retry is pending, it does not** (a `DEAD` row is kept for diagnostics and manual resurrection).

`attempt` is 1-based and counts all attempts for that kind. `maxRetries` in configuration means retries **after** the first attempt, so the budget is `attempt <= maxRetries + 1`.

### D5 — Event status remains coarse: add `ACCEPTED` and `DEAD`

`EventStatus` is not expanded with delivery/processing failure states:

| Status | Meaning |
|---|---|
| `UNDEFINED` | persisted only, not lifecycle-tracked |
| `NEW` | prepared for publication |
| `PUBLISHED` | sent to the transport, awaiting `AcceptedAck` |
| `ACCEPTED` | accepted by the consumer, awaiting processing outcome |
| `HANDLED` | processing completed successfully |
| `FAILED` | failure; details and the pending retry are in `event_retry` |
| `DEAD` | terminal dead-letter status; no automatic retry remains; manual `markForRetry` only |

Notes:

- `ACCEPTED` is the visible delivery/processing boundary and the anchor for the processing watchdog. It is the one new lifecycle status that pays for itself.
- `FAILED` is a **common failure marker**, not a retry-queue discriminator. The scheduler must read `event_retry`, not `event_store.status` alone.
- `DEAD` means **dead-lettered**: it is terminal for automatic retry but manually resurrectable via `markForRetry(eventId)`.
- The status is intentionally named `DEAD`, not `DEAD_LETTERED`, so the event status and `RetryState.DEAD` use the same term for the same concept.
- No `DELIVERY_FAILED` / `PROCESSING_FAILED` statuses. The retry row's `retry_kind` and `reason` provide that detail without widening the status enum.

### D6 — Separate policy domains

Processing and delivery policies are deliberately different types:

- **Processing:** `@RetryPolicy` on the listener, named policies, configuration overrides. The consumer owns the decision.
- **Delivery:** publisher configuration only (`event-flow.retry.delivery.*`). The listener must not know how the publisher transports events.

The publisher never introspects consumer annotations. The consumer never transports a schedule in an ack. The publisher computes `next_attempt_at` from `now + backoff(attempt)` using its own clock, so clock skew between services cannot corrupt the schedule.

`RetryEventPublisher` remains the **immediate in-memory transport retry** (fast network/broker glitches). The durable `DELIVERY` queue is the recovery path after immediate retries are exhausted or after `AcceptedAck` is missing. These are two levels of the same domain, not duplicate mechanisms.

#### D6.1 — Configuration invariant: `PROCESSING` requires `DELIVERY`

The two domains are independent in state, scheduling and metrics, but not in configuration. A processing retry depends on the delivery boundary: without delivery tracking, a processing retry would silently become a delivery retry and the clear separation would be lost.

| `delivery.enabled` | `processing.enabled` | Mode |
|---|---|---|
| `false` | `false` | New retry mechanism off; the legacy scheduler may run |
| `true` | `false` | Delivery-only mode; processing outcomes are recorded but not automatically retried |
| `true` | `true` | Full two-domain mode |
| `false` | `true` | **Invalid; fail fast at startup** |

Startup validation rules:

- If `event-flow.retry.processing.enabled = true` and `event-flow.retry.delivery.enabled = false`, application startup fails with an `IllegalStateException` whose message explains that processing retries require the delivery boundary.
- The check runs before any retry scheduler or `LifecycleRepository` is created. It is a configuration error, not a runtime condition.
- The validation is implemented in a small, unit-testable `RetryConfigurationValidator` and invoked from the Spring lifecycle configuration.

Delivery-only mode semantics:

- `AcceptedAck` is still tracked; delivery retries and the delivery watchdog work.
- On `FailureAck`, the event becomes `FAILED` with error details, but no `PROCESSING` retry row is created. The failure is visible to operators and can be recovered by enabling processing or by `markForRetry`.
- The processing watchdog is disabled. A lost final ack leaves the event in `ACCEPTED` by design; delivery-only mode does not attempt to recover processing.
- The legacy `EventRetryScheduler` must be disabled when the new mechanism is enabled; otherwise it would retry `FAILED` events outside the two-domain model.

### D7 — Schedulers, claiming and watchdogs

Two strategies with independent configuration, sharing one retry store:

**DeliveryRetryScheduler**

1. Find due `DELIVERY` rows: `state = PENDING AND next_attempt_at <= now`.
2. Claim optimistically:
   ```sql
   UPDATE event_retry
      SET state = 'IN_PROGRESS', claimed_by = ?, claimed_until = ?
    WHERE event_id = ? AND retry_kind = 'DELIVERY'
      AND state = 'PENDING' AND next_attempt_at <= now
   ```
3. Load the stored event, deserialize it, restore channels, stamp `retryKind = DELIVERY` and `attempt`, re-publish.
4. If no `AcceptedAck` arrives by `claimed_until`, return the row to `PENDING` and increment `attempt`.
5. On `AcceptedAck`, delete the row.
6. On `maxDeliveryRetries` exhaustion, set the row state to `DEAD` and the event status to `DEAD`.

**ProcessingRetryScheduler**

1. Find due `PROCESSING` rows.
2. Claim and re-publish with `retryKind = PROCESSING` and `attempt`.
3. Wait for `SuccessAck` / `FailureAck`.
4. On success, delete the row and mark the event `HANDLED`.
5. On a retryable failure, return the row to `PENDING` with the next attempt.
6. On a permanent failure or `maxProcessingRetries` exhaustion, set the row state to `DEAD` and the event status to `DEAD`.

Scheduling rules:

- Every scheduler-initiated re-publish stamps `retryKind` and `attempt` into the Envelope metadata. The consumer echoes them in `SuccessAck` / `FailureAck`; `retryKind` identifies the retry row/context, not the semantic ack domain.
- A scheduler-initiated `PROCESSING` re-publish that fails at the transport stage is recorded against the `PROCESSING` row with reason `PUBLISH_FAILED`, not as a new `DELIVERY` row. The retry's purpose remains processing; the delivery failure is the cause.
- If both a `DELIVERY` and a `PROCESSING` row are due for the same event, group by `event_id` in a cycle and perform one re-publish. The same delivery can satisfy the delivery ack and provide the processing attempt.
- Watchdogs avoid a row per healthy event:
  - scan `event_store` for `PUBLISHED` older than `deliveryAckTimeout` → create or raise a `DELIVERY` row;
  - scan `event_store` for `ACCEPTED` older than `processingAckTimeout` → create or raise a `PROCESSING` row.
- `claimed_until` must exceed the maximum expected publish + ack window. An expired lease returns the row to `PENDING`; this is at-least-once, not exactly-once.
- Manual `markForRetry(eventId)` resets the relevant row to `PENDING` and the event to `FAILED`/`NEW` atomically. It bypasses the current backoff but not the hard safety cap unless an explicit operator override is configured.

### D8 — Reliability invariants

1. **Hard caps per kind.** `maxDeliveryRetries` and `maxProcessingRetries` are absolute ceilings. Even a consumer that always reports `retryable = true` cannot cause an infinite loop.
2. **Jitter.** Every durable backoff uses jitter (full jitter or ±15%) to avoid re-publish herds.
3. **Conditional, out-of-order-safe status transitions.** `AcceptedAck` may arrive before or after `EventLifecyclePublisher` records `PUBLISHED`, and `SuccessAck` may arrive before `AcceptedAck`. Status updates must be conditional:
   - `NEW → PUBLISHED → ACCEPTED → HANDLED` is the normal forward path;
   - `HANDLED` and `DEAD` are terminal and are never overwritten by acks;
   - `AcceptedAck` transitions to `ACCEPTED` only from `NEW`/`PUBLISHED`; from `FAILED` it only clears the `DELIVERY` row and leaves the status unchanged;
   - a late `PUBLISHED` must never overwrite `ACCEPTED`, `HANDLED` or `DEAD`.
4. **Stale-ack protection.** Acks carry `retryKind` and `attempt`; an ack with an older attempt than the current row is ignored. This prevents an old `FailureAck` from overwriting a newer success.

   A `DELIVERY` row is cleared by `AcceptedAck` and by any processing ack (`SuccessAck`, `FailureAck`, `NoHandlerAck`), because all of them prove delivery. A late `AcceptedAck` after a processing ack must not leave a delivery row behind.
5. **Idempotency correction.** `IdempotentEventDispatcher` must invalidate the event-id cache on **any** handler failure (`!isAllSuccess()`), not only on `isAllFailure()`. Otherwise a partial failure is cached and the processing retry is dropped as a duplicate.
6. **Processing timeout vs local retry.** The processing timeout must exceed the maximum local retry budget. Use `AcceptedAck.maxProcessingMs` when present, otherwise the configured global value plus margin.
7. **Cleanup safety.** `EventCleanupScheduler` must not delete an `event_store` row while an `event_retry` row exists for it. Prefer an FK with `ON DELETE CASCADE` plus a cleanup predicate `NOT EXISTS (SELECT 1 FROM event_retry WHERE event_id = ...)`. Dead-letter rows get a separate, longer retention.
8. **Transactionality.** `event_store` and `event_retry` updates for one lifecycle transition happen in one transaction (D9).
9. **No new failure event.** `FailureAck`, `NoHandlerAck` and `AcceptedAck` are technical `LifecycleAckEvent`s. Subscribers can feed metrics, alerting or a DLQ sink from them. A dedicated `EventProcessingFailed` is unnecessary and would only add a second loop surface.

### D9 — Transactional lifecycle boundary

`LifecycleRepository` (or an equivalent transactional facade) owns coordinated writes to `event_store` and `event_retry`. Direct calls from the publisher/ack handler to two independent stores would permit states such as:

- `FAILED` without a retry row;
- a retry row for an event that is already `HANDLED`;
- a dead retry row without a visible failed event.

The repository is intentionally small:

```java
public interface LifecycleRepository {
    void onPublishFailed(UUID eventId, String error, RetryReason reason);
    void onAccepted(UUID eventId, int attempt, long maxProcessingMs);
    void onProcessingFailed(UUID eventId, int attempt, boolean retryable,
                            List<String> failedHandlers, String error, String errorType);
    void onHandled(UUID eventId, int attempt);
    void onNoHandler(UUID eventId, int attempt);
    void onDeadLetter(UUID eventId, RetryKind kind, RetryReason reason, String error);
    void markForRetry(UUID eventId);
}
```

Implementation notes:

- For JDBC stores, the repository uses one connection/transaction per transition.
- For `InMemoryEventStore`, a single lock provides the same atomicity.
- For custom stores, the repository falls back to the legacy scheduler or requires the custom store to implement the retry-store SPI. This is documented in the release notes.

### D10 — Backward compatibility

- Listeners without `@RetryPolicy`: no local retry; failure produces `FailureAck(retryable = true)` → durable processing retry. Today's behaviour is preserved.
- Legacy `FailureAck` constructors: `retryKind = PROCESSING`, `retryable = true`, `attempt = 1`.
- A publisher receiving `AcceptedAck` from a new consumer: it processes the ack; a publisher that does not understand it treats it as an unknown technical event and ignores it.
- A new publisher with an old consumer: no `AcceptedAck` is emitted. Configuration `require-accepted-ack = false` makes publish success equivalent to `ACCEPTED` (legacy behaviour). With `require-accepted-ack = true`, a missing `AcceptedAck` triggers a delivery retry. The default must be chosen per upgrade: `false` for a mixed fleet, `true` once consumers are upgraded.
- Old `FAILED` rows without `event_retry` rows: the legacy scheduler heuristic handles them during a transition, or a startup backfill maps them to a `PROCESSING` retry row (the subsequent action is a re-publish either way). The kind is diagnostic; the recovery action is the same.
- `retry_count` in `event_store` is kept for dashboards and legacy stores but is no longer the authoritative budget. The `event_retry.attempt` is authoritative.
- New API additions use default methods or retained constructors: `@EventListener.retryPolicy()`, `EventHandler.retryPolicy()`, new ack components, new status values.

### D11 — Observability and dead-letter retention

Metrics:

- `eventflow_retry_pending{kind}`;
- `eventflow_retry_attempts_total{kind, reason}`;
- `eventflow_ack_timeout_total{kind}`;
- `eventflow_dead_lettered_total{kind, reason}`;
- `eventflow_processing_duration` (including local retries).

Logs include `eventId`, `retryKind`, `attempt`, `reason`, `failedHandlers`, `policyName` and destination where applicable.

Operations:

- An overview view joins `event_store` and `event_retry` so `FAILED` is never ambiguous in dashboards.
- `DEAD` has its own retention, longer than `HANDLED`/`UNDEFINED`, so dead letters survive long enough for triage and `markForRetry(eventId)`.
- A `DeadLetteredEvent` carrying the payload to a DLQ channel is deferred. In the MVP, `FailureAck` / `NoHandlerAck` are subscribable and the store row holds the payload.

### D12 — Deferred scope

Deferred until the MVP proves the model:

- per-handler durable isolation and `PARTIALLY_HANDLED`;
- `targetHandlers` metadata to re-run only failed handlers on a durable processing retry;
- per-destination delivery rows and retrying only failed channels;
- `event_retry_policy` table, DB seeding and runtime policy tuning without redeploy;
- a DLQ event with the full payload;
- a dedicated retry executor (the async delayed-executor approach is sufficient initially);
- `ignoreRetryOn` beyond the `noRetryOn` blacklist.

## State machine and write responsibilities

| Situation | `event_store.status` | `event_retry.retry_kind` | `event_retry.state` | Writer |
|---|---|---|---|---|
| first publish, before transport result | `NEW` | — | — | `EventLifecyclePublisher` |
| publish failed / all destinations failed | `FAILED` | `DELIVERY` | `PENDING` | `LifecycleRepository` on publish failure |
| published, no `AcceptedAck` by timeout | `PUBLISHED` | `DELIVERY` | `PENDING` | watchdog |
| `AcceptedAck` received, current `NEW`/`PUBLISHED` | `ACCEPTED` | — | row deleted | `AckHandler` |
| `AcceptedAck` received, current `FAILED` | unchanged (`FAILED`) | — | `DELIVERY` row deleted | `AckHandler` |
| no handler resolved | `DEAD` | `PROCESSING` | `DEAD` | `AckHandler` on `NoHandlerAck` |
| handler failed, retryable | `FAILED` | `PROCESSING` | `PENDING` | `AckHandler` on `FailureAck` |
| handler failed, permanent | `DEAD` | `PROCESSING` | `DEAD` | `AckHandler` on `FailureAck` |
| accepted, no final ack by timeout | `ACCEPTED` | `PROCESSING` | `PENDING` | watchdog |
| success after retry | `HANDLED` | — | row deleted | `AckHandler` on `SuccessAck` |
| budget exhausted | `DEAD` | same as failed domain | `DEAD` | scheduler |
| manual retry after `DEAD` | `FAILED` → `NEW` | same kind | `PENDING` | operator / `markForRetry` |

Invariant: a `DELIVERY` row is deleted on `AcceptedAck` **or any processing ack** (`SuccessAck`, `FailureAck`, `NoHandlerAck`), because every processing ack proves delivery. A `PROCESSING` row is deleted only on `SuccessAck`. A `DEAD` row is retained for diagnostics and manual resurrection.

## Ack flow

```text
Publisher                                      Dispatcher
    |                                              |
    |-- publish ---------------------------------->|
    |                                              | received
    |<--------------- AcceptedAck ------------------|   (before handlers)
    |                                              | resolve handlers
    |                                              | execute (with local retries)
    |<--------------- SuccessAck -------------------|
    |<--------------- FailureAck -------------------|
    |<--------------- NoHandlerAck -----------------|   (no handler)
    |                                              |
```

- `AcceptedAck` and `NoHandlerAck` cannot both describe a successful resolution; `NoHandlerAck` is the permanent alternative.
- `AcceptedAck` closes delivery only.
- `SuccessAck` / `FailureAck` close processing.
- Lost acks produce duplicates; this is accepted at-least-once behaviour and is mitigated by idempotent handling and stale-ack guards.

## Component change map

| Component | Change |
|---|---|
| **new** `RetryKind` | `DELIVERY`, `PROCESSING` |
| **new** `RetryState` | `PENDING`, `IN_PROGRESS`, `DEAD` |
| **new** `RetryReason` | `PUBLISH_FAILED`, `PARTIAL_PUBLISH_FAILED`, `ACK_TIMEOUT`, `HANDLER_FAILED`, `PROCESSING_TIMEOUT`, `NO_HANDLER`, `CAP_EXCEEDED` |
| **new** `AcceptedAck` | delivery confirmation, `attempt`, `maxProcessingMs` |
| **new** `NoHandlerAck` | permanent configuration failure |
| `FailureAck` | + `retryKind`, `attempt`, `retryable`, `failedHandlers`, `errorType`; legacy constructor retained |
| `SuccessAck` | + `retryKind`, `attempt`; legacy constructor retained |
| **new** `@RetryPolicy` | `retryOn[]`, `noRetryOn[]`, `maxRetries`, `initialDelay`, `unit`, `multiplier`, `maxDelay`, `jitter`, `name`; targets `METHOD, TYPE` |
| `@EventListener` | + `retryPolicy()` (policy name) |
| **new** `RetryPolicyRegistry` | startup scan of method/type annotations; fail-fast on unknown names |
| `EventHandler` | + `default RetryPolicy retryPolicy()` (backward-compatible) |
| `MethodInvokingEventHandler` / `EventListenerRegistry` | resolve and carry the policy |
| `UnifiedEventDispatcher` | async local retry loop, per-attempt semaphore, classification |
| `HandlerResult` / `HandlerResults` | + `retryable`, `failureKind`, `attempts`, `policyName`; aggregate predicates |
| `EventLifecycleDispatcher` | emit `AcceptedAck` before handlers; emit `NoHandlerAck` when empty |
| `AckHandler` | route `AcceptedAck`, `NoHandlerAck`, `SuccessAck`, `FailureAck` to `LifecycleRepository`; stale-ack guard |
| `EventLifecyclePublisher` | create `DELIVERY` retry on publish failure; stamp `retryKind` / `attempt`; monotonic status updates |
| **new** `EventRetryStore` | retry-row persistence: schedule, claim, reschedule, complete, dead-letter |
| **new** `RetryTask` | event id, kind, attempt, next attempt, state, reason, claim metadata |
| **new** `LifecycleRepository` | transactional coordination of `event_store` + `event_retry` |
| **new** `DeliveryRetryScheduler` | durable delivery retries, watchdog for missing `AcceptedAck` |
| **new** `ProcessingRetryScheduler` | durable processing retries, watchdog for missing final ack |
| `EventStatus` | + `ACCEPTED('A')`, + `DEAD('D')`; status comments updated |
| `EventStore` | no interface change; built-in stores implement `EventRetryStore` (or a combined repository) |
| `JdbcEventStore` / `InMemoryEventStore` | retry table support / in-memory retry map |
| 5 SQL dialects + DDL scripts + `SchemaInitializer` | `event_retry` table, indexes, status comments |
| `EventRetryScheduler` | deprecated in favour of the two schedulers; legacy mode retained for old rows |
| `EventCleanupScheduler` | exclude events with pending `event_retry`; separate `DEAD` retention |
| `IdempotentEventDispatcher` | invalidate cache on any handler failure |
| `EventFlowProperties` | new `event-flow.retry.delivery.*`, `event-flow.retry.processing.*`; startup validation (`RetryConfigurationValidator`); legacy properties aliased/deprecated |
| `RetryEventPublisher` | unchanged: immediate in-memory transport retry |
| `TransactionalEventPublisher` | unchanged |

## Schema changes

### `event_retry`

See D4 for the canonical PostgreSQL DDL. For other dialects, follow the existing `event_store` conventions:

- UUID via the existing `UuidType` abstraction (MySQL/ SQL Server may use the configured UUID representation);
- timestamps via `TIMESTAMP` / `DATETIME2`;
- Java `String` payloads via `VARCHAR` / `NVARCHAR`;
- long errors via `TEXT` / `CLOB` / `NVARCHAR(MAX)`.

`SchemaInitializer` runs `CREATE TABLE IF NOT EXISTS event_retry` and the two indexes. No `ALTER TABLE event_store` is required for retry state.

### `event_store`

No new columns. Only status codes and comments change:

```sql
-- PostgreSQL
COMMENT ON COLUMN event_store.status IS
    'U=UNDEFINED, N=NEW, P=PUBLISHED, A=ACCEPTED, H=HANDLED, F=FAILED, D=DEAD';
```

### Migration for existing installations

- Create `event_retry` on startup when `auto-init-schema = true`; in verify-only mode fail fast with a message pointing to the release notes.
- Document the `CREATE TABLE` snippet for all five dialects in the release notes.
- Keep the legacy scheduler heuristic for pre-upgrade `FAILED`/`PUBLISHED` rows during a transition. Optionally backfill them as `PROCESSING` `PENDING` rows; the recovery action is a re-publish either way.
- Do not delete old `FAILED` rows until they have a retry row or are explicitly dead-lettered.

## Full cycle example

Scenario: payment service, `InternalEventChannel`, `MANAGED` lifecycle, Kafka transport.

```java
@Event(channels = InternalEventChannel.class, lifecycle = EventLifecycle.MANAGED)
public record OrderCreated(String orderId, String customerId, long amount) {}

@EventListener(OrderCreated.class)
@RetryPolicy(retryOn = TimeoutException.class, maxRetries = 3,
        initialDelay = "100ms", multiplier = 2.0, maxDelay = "5s")
public void charge(OrderCreated e) { gateway.charge(e); }

@EventListener(OrderCreated.class)
public void audit(OrderCreated e) { auditLog.append(e); }
```

**Cycle A — transient handler failure absorbed locally:**

```text
publish -> event_store: NEW -> transport -> dispatcher
    AcceptedAck -> event_store: ACCEPTED
    charge: TimeoutException, TimeoutException, ok  (local retries)
    audit: ok
    SuccessAck -> event_store: HANDLED
```

No store round-trip, no re-run of `audit`, no delivery retry.

**Cycle B — local budget exhausted -> durable processing retry:**

```text
publish -> AcceptedAck -> event_store: ACCEPTED
    charge: TimeoutException x4 (local budget exhausted)
    audit: ok
    FailureAck(retryKind=PROCESSING, retryable=true, attempt=1)
        -> event_store: FAILED
        -> event_retry(PROCESSING, PENDING, next_attempt_at = now + backoff)

ProcessingRetryScheduler:
    re-publish with retryKind=PROCESSING, attempt=2
    AcceptedAck -> wait for final ack
    charge: ok on first try
    audit: ok (re-run; idempotency required)
    SuccessAck -> event_store: HANDLED, row deleted
```

**Cycle C — permanent handler failure -> immediate dead-letter:**

```text
publish -> AcceptedAck -> event_store: ACCEPTED
    charge: PaymentRejectedException (not in retryOn) -> permanent
    audit: ok
    FailureAck(retryKind=PROCESSING, retryable=false, attempt=1)
        -> event_store: DEAD
        -> event_retry(PROCESSING, DEAD, reason=HANDLER_FAILED)
```

No re-publish, `audit` is never re-run. An operator fixes the data and calls `markForRetry(eventId)`; the row becomes `PENDING` and the event is resurrected.

**Cycle D — delivery failure / lost acceptance:**

```text
publish -> transport exception
    -> event_store: FAILED
    -> event_retry(DELIVERY, PENDING, reason=PUBLISH_FAILED)

DeliveryRetryScheduler:
    re-publish
    no AcceptedAck by timeout -> row back to PENDING, attempt++
    AcceptedAck -> row deleted, event_store: ACCEPTED
```

**Cycle E — no handler:**

```text
publish -> AcceptedAck -> event_store: ACCEPTED
    no handler resolved
    NoHandlerAck
        -> event_store: DEAD
        -> event_retry(PROCESSING, DEAD, reason=NO_HANDLER)
```

Alerting sees the dead letter; a redeploy with the missing handler plus `markForRetry` recovers it.

## Consequences

### Positive

- Delivery and processing failures become distinguishable by evidence, not by guessing.
- A `publish()` exception no longer automatically causes a duplicate delivery if the consumer actually accepted the event.
- The common case (transient handler failure) is absorbed locally in milliseconds without touching the store or other listeners.
- Durable recovery remains event-level and crash-survivable, with a single retry table and no schedule transported in acks.
- Per-listener flexibility lives in code and configuration; no database policy table is required in phase 1.
- Hard caps, jitter, leases and monotonic transitions close the infinite-loop, thundering-herd and stale-ack holes.
- `EventStatus` stays coarse; no schema-wide status migration.
- `FAILED` + `event_retry` gives a joinable, inspectable operational view.
- Loop safety reuses `LifecycleAckEvent`; no new `SystemEvent` marker.

### Negative / trade-offs

- Adds a consumer-side `AcceptedAck` emission point and a new technical ack.
- Adds a retry table, a transactional repository and claim/lease logic.
- `AcceptedAck` can itself be lost, so duplicate delivery remains possible (at-least-once).
- Durable processing retry re-runs all handlers; per-handler isolation and `PARTIALLY_HANDLED` are deferred.
- `AcceptedAck` before handler execution means a lost acceptance can cause a duplicate processing attempt.
- Processing latency is bounded below by the scheduler interval for durable retries.
- The processing timeout must be sized against local retry budgets; a misconfiguration can cause premature retries.
- Existing dashboards that interpret `FAILED` alone remain ambiguous until they join `event_retry`.

## Alternatives considered

- **ADR-0001 schedule-in-ack.** Rejected: transports a schedule, requires clock-skew and mixed-fleet handling, adds `EventProcessingFailed` and `SystemEvent`, and lets the publisher lose its hard cap.
- **ADR-0002 local-only retry.** Rejected as the whole solution: not crash-survivable and does not distinguish delivery from processing. Its local layer is retained.
- **ADR-0003 two tables + DB policy table in phase 1.** Rejected for the MVP: unnecessary surface area. Its boundary and retry-kind model are retained; storage is reduced to one table and policy storage is deferred.
- **No `AcceptedAck`; broker ack equals delivery.** Rejected: broker acceptance does not prove consumer receipt, and a lost final ack is indistinguishable from a lost delivery. The user requirement is an explicit separation.
- **Per-handler durable rows in the MVP.** Deferred: local per-listener retries already provide per-listener flexibility; durable processing retry is the crash-recovery fallback and may re-run all handlers with idempotency.
- **Two separate retry tables.** Rejected: one kind-discriminated table gives one scheduler query and one store abstraction.
- **A new `EventProcessingFailed` event.** Rejected: `FailureAck` / `NoHandlerAck` are already `LifecycleAckEvent`s and can feed metrics, alerting and a DLQ sink without a second loop surface.

## Open questions

| # | Question | Recommendation |
|---|---|---|
| 1 | Should acks carry `attempt`, `deliveryId`, or both? | `attempt` + `retryKind` for the MVP; add `deliveryId` if per-channel correlation is needed |
| 2 | Is `(event_id, retry_kind)` sufficient, or is per-target keying needed? | Sufficient for the MVP; extend to `(event_id, retry_kind, target)` in phase 2 |
| 3 | Does manual retry reset the attempt budget? | Reset the relevant kind's attempt budget; keep an audit trail in logs |
| 4 | Should no-handler get a short grace period during rolling deploys? | Keep it permanent in the MVP; operators use `markForRetry` after the deploy |
| 5 | What are the dead-letter retention defaults? | Longer than `HANDLED`; make it explicitly configurable |
| 6 | Should `require-accepted-ack` default to true or false? | `false` during migration, `true` once all consumers are upgraded |
| 7 | Should old `FAILED` rows be backfilled or handled by the legacy heuristic? | Legacy heuristic during the transition; optional backfill as a maintenance task |
| 8 | Is the optimistic claim enough for all five dialects? | Yes for the MVP; consider `SKIP LOCKED` / `READPAST` as an optimization |
| 9 | Do we keep `retry_count` in `event_store`? | Keep for dashboards/legacy, but `event_retry.attempt` is authoritative |
| 10 | Should a `PROCESSING` re-publish that fails at the transport stage create a `DELIVERY` row? | No: record it against the `PROCESSING` row with reason `PUBLISH_FAILED` |

## Required regression tests

1. Publish failure creates a `DELIVERY` row; `AcceptedAck` deletes it.
2. Publish success without `AcceptedAck` eventually creates a delivery retry.
3. A transient handler failure is retried locally and succeeds without a store round-trip; other handlers run once.
4. Local budget exhausted produces `FailureAck(retryable = true)` and a `PROCESSING` row.
5. A permanent handler failure produces `FailureAck(retryable = false)` and `DEAD` without a re-publish.
6. No handler produces `NoHandlerAck` and `DEAD`, not `SuccessAck`.
7. A lost final ack triggers a processing retry after the timeout.
8. A stale `FailureAck` from an older attempt is ignored after a newer success.
9. `AcceptedAck` arriving before the `PUBLISHED` update does not get overwritten.
10. Both delivery and processing rows pending for one event produce a single re-publish per cycle.
11. Global caps terminate an always-retryable consumer in `DEAD`.
12. Two scheduler instances cannot process the same row concurrently (lease/claim).
13. `IdempotentEventDispatcher` invalidates the cache on partial failure, so the processing retry runs.
14. Cleanup does not delete an event with a pending retry row or a dead letter within retention.
15. `LifecycleRepository` rolls back both writes when one side fails.
16. Backward compatibility: no `@RetryPolicy`, legacy `FailureAck` constructor, old `FAILED` rows, mixed-fleet `require-accepted-ack = false`.
17. `processing.enabled = true` with `delivery.enabled = false` fails startup with a clear configuration error.
18. Delivery-only mode records a handler failure as `FAILED` without creating a `PROCESSING` row and without running a processing retry.
19. `SuccessAck` arriving before `AcceptedAck` moves the event to `HANDLED`; the late `AcceptedAck` is ignored for status and does not leave a delivery row.
20. `FailureAck` arriving before `AcceptedAck` keeps the event `FAILED` with a `PROCESSING` row; the late `AcceptedAck` only clears the delivery row and does not change the status.
21. A late publish result must not overwrite `HANDLED`, `ACCEPTED` or `DEAD`.
22. A `SuccessAck` with `retryKind = DELIVERY` closes the delivery row and marks the event `HANDLED`; a stale success with an older attempt is ignored.
23. A `FailureAck` with `retryKind = DELIVERY` closes the delivery row and creates a `PROCESSING` retry row.
24. A first-publish ack without retry metadata defaults to `PROCESSING` / `attempt = 1` and does not require an existing retry row.

## Implementation phases

**Phase 1 — delivery/processing boundary (no schema change).**
`AcceptedAck`, `NoHandlerAck`, `RetryKind`, `HandlerResult` / `HandlerResults` classification, `FailureAck` / `SuccessAck` extensions, `EventLifecycleDispatcher` emission, stale-ack guards, `IdempotentEventDispatcher` invalidation fix, metrics and logs.

**Phase 2 — local processing retry.**
`@RetryPolicy`, `RetryPolicyRegistry`, named policies, configuration overrides, async local retry loop, backoff + jitter, global local cap, `FailureAck.retryable`.

**Phase 3 — durable retry queue.**
`event_retry` table, `EventRetryStore`, `LifecycleRepository`, `ACCEPTED` / `DEAD` statuses, `DeliveryRetryScheduler`, `ProcessingRetryScheduler`, watchdogs, claim/lease, caps, `AckHandler` routing, `EventLifecyclePublisher` delivery-row creation and attempt stamping, `RetryConfigurationValidator` startup validation, schema initialization and migration.

**Phase 4 — operations and deferred features.**
Dead-letter retention, manual retry, overview view, optional backfill, per-handler durable isolation / `PARTIALLY_HANDLED`, per-destination delivery, DB policy table and seeding, DLQ event with payload.
