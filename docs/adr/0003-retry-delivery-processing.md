# ADR-0003: Delivery Confirmation, Processing Failure and Retry Policy Storage

- **Status:** Superseded by [ADR-0004](0004-two-domain-event-retry.md) (Proposed) — its delivery/processing boundary and `event_retry` model are retained in simplified form
- **Date:** 2026-10-31
- **Author:** project maintainer (design review)
- **Relates to:** [ADR-0002](0002-listener-retry-two-layer.md) — extended the two-layer model and superseded its `DEAD_LETTERED` status decision (D4/D6). [ADR-0001](0001-listener-retry-policies.md) is already superseded by ADR-0002.
- **Tags:** retry, lifecycle, delivery, processing, dead-letter, event-store, policy, ack

## Context

### The current ambiguity

The lifecycle currently has one failure status, `FAILED`, and one retry scheduler that treats `FAILED`, `PUBLISHED` and `NEW` as retry candidates. This conflates two different questions:

1. **Was the event delivered to a consumer?**
2. **Did the consumer's handler process it successfully?**

A `publish()` exception does not answer the first question. The broker may already have accepted and delivered the event; the exception may be a lost acknowledgement, a timeout after a successful send, or a partial-channel failure. Treating every `publish()` exception as "not delivered" causes unnecessary duplicate deliveries.

Conversely, a missing final ack does not prove that processing failed. It may mean the event was never delivered, the consumer crashed before handling it, or the ack itself was lost.

### What is needed

A single, explicit boundary between:

- **delivery** — transport/consumer acceptance;
- **processing** — handler execution after acceptance.

Without a consumer-side acceptance signal, the two cannot be distinguished reliably. The existing `SuccessAck`/`FailureAck` are emitted only after handler execution, so they answer the processing question but not the delivery question.

## Goals

- Define delivery success by an explicit consumer-side acceptance event, not by the absence of a `publish()` exception.
- Keep the existing `EventStatus` model; `FAILED` remains a coarse failure status.
- Store retry details and configurable policies in dedicated tables, not in `EventStatus`.
- Allow operational policy overrides without erasing the developer's intent in code.
- Preserve at-least-once semantics; exactly-once delivery remains out of scope.

## Non-Goals

- Exactly-once processing.
- Replacing the existing event store.
- Implementing the design in this ADR. Code is intentionally untouched until the remaining open questions are resolved.
- A separate table for exception filters in the first iteration.
- Database policy storage in phase 1. Named policies are overridden through configuration properties/YAML; the `event_retry_policy` table is deferred to a later phase.
- Per-handler durable isolation (`PARTIALLY_HANDLED`); still a phase-2 candidate.

## Confirmed Decisions

### D1 — `AcceptedAck` is the delivery/processing boundary

Delivery is considered successful only when the consumer-side dispatcher has:

1. received the event;
2. resolved at least one handler for it;
3. emitted `AcceptedAck` before invoking the handler(s).

```text
publish
  -> transport accepted
  -> dispatcher received the event
  -> dispatcher resolved handler(s)
  -> AcceptedAck                 <-- delivery confirmed here
  -> handler execution
  -> SuccessAck / FailureAck     <-- processing outcome
```

Consequences:

- A `publish()` exception is **delivery uncertainty**, not proof of non-delivery.
- If the event actually reached the dispatcher, `AcceptedAck` cancels the pending delivery retry.
- If no `AcceptedAck` arrives within the delivery timeout, the event is retried as a delivery failure.
- Delivery retry and processing retry are separate domains and are recorded separately.
- `AcceptedAck` is a technical event (`LifecycleAckEvent`); it is not persisted and gets no lifecycle acks.

`AcceptedAck` is only an acceptance signal. It does not mean the handler succeeded.

### D2 — No handler is a permanent configuration failure

If the dispatcher receives an event but resolves **no handler**, it emits `NoHandlerAck` instead of today's implicit success.

`NoHandlerAck` moves the event to the terminal `DEAD` retry state:

- it is not retried automatically;
- it is visible to operators and alerting;
- it can be recovered manually after the missing handler/configuration is fixed.

A missing handler is not a transient delivery failure. Retrying it would only produce the same result.

### D3 — Policy precedence: inline code wins, named policy is overridable

`@RetryPolicy` supports two placement modes.

**Inline policy — authoritative:**

```java
@EventListener(OrderCreated.class)
@RetryPolicy(retryOn = TimeoutException.class, maxRetries = 3)
public void on(OrderCreated event) { ... }
```

An inline policy on the listener method is an explicit code-level decision. The database **does not override it** by default. A future `overridable = true` opt-in may be added, but it is not part of this decision.

**Named policy — overridable:**

```java
@RetryPolicy(name = "payment-retry",
        retryOn = PaymentUnavailableException.class,
        maxRetries = 5)
public class RetryPolicies { }

@EventListener(value = PaymentFailedEvent.class, retryPolicy = "payment-retry")
public void on(PaymentFailedEvent event) { ... }
```

For a named policy:

1. a configured override with the same name is used when present and enabled (phase 1: properties/YAML; phase 3: an `event_retry_policy` row);
2. otherwise the code declaration is used;
3. if neither exists, global defaults apply.

The annotation attribute on `@EventListener` is named `retryPolicy` (camelCase; Java identifiers cannot contain hyphens).

Phase 1 overrides live in configuration properties, not in a database table:

```properties
event-flow.retry.policies.payment-retry.max-retries=5
event-flow.retry.policies.payment-retry.initial-delay-ms=200
event-flow.retry.policies.payment-retry.multiplier=2.0
event-flow.retry.policies.payment-retry.max-delay-ms=5000
event-flow.retry.policies.payment-retry.retry-on[0]=io.example.PaymentUnavailableException
event-flow.retry.policies.payment-retry.no-retry-on[0]=io.example.PaymentRejectedException
```

This removes schema, caching and seeding concerns from the first phase. The trade-off is that policy changes require a restart (or a config refresh if the runtime supports it).

## Proposed Design (Pending Review)

The following parts are the working model, but are not yet confirmed as final decisions.

### Event status remains coarse

`EventStatus` is not extended with delivery/processing/dead-letter states:

| Status | Meaning |
|---|---|
| `UNDEFINED` | persisted only, not lifecycle-tracked |
| `NEW` | prepared for publication |
| `PUBLISHED` | published/awaiting consumer outcome |
| `HANDLED` | processing completed successfully |
| `FAILED` | failed; details are in `event_retry` |

`FAILED` becomes a **common failure marker**, not a retry queue discriminator. The retry scheduler must not select work by `event_store.status` alone; it reads `event_retry`.

`DEAD` is a retry-state value in `event_retry`, not an `EventStatus`. This avoids a schema-wide status migration and keeps terminal-retry concerns in one place.

### `event_retry` — retry work queue

The table is the source of truth for a pending retry: attempt number, next attempt time, domain, state and last error.

```sql
CREATE TABLE event_retry (
    event_id          UUID PRIMARY KEY,
    service           VARCHAR(255) NOT NULL,
    retry_kind        VARCHAR(16)  NOT NULL,   -- DELIVERY | PROCESSING
    state             VARCHAR(16)  NOT NULL,   -- PENDING | LEASED | DEAD
    attempt           INT          NOT NULL DEFAULT 0,
    next_attempt_at   TIMESTAMP    NOT NULL,
    reason            VARCHAR(64),             -- PUBLISH_FAILED | ACK_TIMEOUT | HANDLER_FAILED | PROCESSING_TIMEOUT | NO_HANDLER
    last_error_type   VARCHAR(512),
    last_error        TEXT,
    policy_name       VARCHAR(255),
    policy_version    BIGINT,
    claimed_by        VARCHAR(255),
    claimed_until     TIMESTAMP,
    created_at        TIMESTAMP    NOT NULL,
    updated_at        TIMESTAMP    NOT NULL
);

CREATE INDEX idx_event_retry_due
    ON event_retry (state, next_attempt_at);

CREATE INDEX idx_event_retry_service_due
    ON event_retry (service, state, next_attempt_at);
```

Open points:

- Whether one row per event is sufficient, or history requires a generated `id` plus a unique active row per `(event_id, retry_kind)`.
- Whether an `AcceptedAck` deletes the delivery row or marks it done.
- Whether ack-timeout detection uses a pre-created delivery row or a separate scheduler query over `PUBLISHED`.

The intended invariant is: **if a retry is pending, `event_retry` has a row; if no retry is pending, it does not.**

### `event_retry_policy` — operational policy overrides (deferred to phase 3)

Phase 1 does **not** create this table. Named policies are overridden via properties/YAML (see D3). The table below is the phase-3 target, when runtime tuning without redeploy becomes necessary.

```sql
CREATE TABLE event_retry_policy (
    policy_name       VARCHAR(255) PRIMARY KEY,
    event_type        VARCHAR(512),
    handler_name      VARCHAR(512),
    enabled           BOOLEAN      NOT NULL DEFAULT TRUE,
    max_retries       INT          NOT NULL,
    initial_delay_ms  BIGINT       NOT NULL,
    multiplier        DECIMAL(6,3) NOT NULL,
    max_delay_ms      BIGINT       NOT NULL,
    retry_on          TEXT,
    no_retry_on       TEXT,
    source            VARCHAR(16)  NOT NULL,   -- CODE | DB
    version           BIGINT       NOT NULL DEFAULT 1,
    created_at        TIMESTAMP    NOT NULL,
    updated_at        TIMESTAMP    NOT NULL
);

CREATE INDEX idx_event_retry_policy_lookup
    ON event_retry_policy (event_type, handler_name, enabled);
```

`retry_on` / `no_retry_on` are stored as text in the first iteration, for example a JSON array or newline-separated FQCN list. A child table is not required until normalized querying, per-rule editing or audit becomes a real requirement.

Policy rows are cached by the consumer (suggested TTL: 30 seconds). A missing or broken row must fall back to the code declaration/global defaults with a WARN and a metric; it must not fail application startup or message handling.

`policy_version` is copied to `event_retry` for diagnostics: it explains which version of a policy scheduled the current attempt.

### Seeding policies from code (deferred to phase 3)

On startup, named `@RetryPolicy` declarations may be inserted into `event_retry_policy` if absent:

```text
insert missing rows only
never overwrite existing rows
never delete rows
source = CODE
```

This is bootstrap, not continuous synchronization. An operator's database changes must survive application restarts. The behavior is controlled by a flag such as `event-flow.retry.policy.seed-from-code`.

### Transactional lifecycle boundary

A `LifecycleRepository` (or equivalent transactional facade) owns coordinated writes to `event_store` and `event_retry`. Direct calls from the publisher/ack handler to two independent stores would allow states such as:

- `FAILED` without a retry row;
- a retry row for an event that is already `HANDLED`;
- a dead retry row without a visible failed event.

The exact interface is pending, but `EventStore` alone is no longer sufficient for retry state transitions.

### Multi-instance claiming

A scheduler instance must claim work before processing it:

```text
PENDING -> LEASED (claimed_by, claimed_until)
LEASED  -> DEAD    on terminal failure
LEASED  -> deleted on success
lease expires      -> PENDING again
```

The claim timeout must exceed the maximum expected publish/ack window. This prevents two instances from retrying the same event simultaneously; it does not replace consumer idempotency.

## State Machine (Proposed)

| Situation | `event_store.status` | `event_retry.retry_kind` | `event_retry.state` |
|---|---|---|---|
| publish failed / unknown | `FAILED` | `DELIVERY` | `PENDING` |
| published, no `AcceptedAck` by timeout | `PUBLISHED` | `DELIVERY` | `PENDING` |
| accepted, handler failed | `FAILED` | `PROCESSING` | `PENDING` |
| accepted, handler succeeded | `HANDLED` | — | row deleted |
| accepted, no final ack by processing timeout | `PUBLISHED`/`FAILED` (open) | `PROCESSING` | `PENDING` |
| no handler found | `FAILED` | `PROCESSING` | `DEAD` |
| budget exhausted / permanent failure | `FAILED` | `DELIVERY`/`PROCESSING` | `DEAD` |
| manual retry after `DEAD` | `FAILED` -> `NEW` | same kind | `PENDING` |

The manual-retry path must reset both the event state and the retry row atomically. It must also clear any manual retry flag so a retried event cannot be claimed twice.

## Ack Flow (Proposed)

```text
Publisher                         Dispatcher
    |                                  |
    |-- publish ---------------------->|
    |                                  | resolve handlers
    |<-------------- AcceptedAck ------|   (before handler execution)
    |                                  | execute handlers
    |<-------------- SuccessAck --------|
    |<-------------- FailureAck --------|
    |                                  |
    |-- no handler ------------------->|
    |<-------------- NoHandlerAck ------|
```

- `AcceptedAck` and `NoHandlerAck` are mutually exclusive.
- `AcceptedAck` closes the delivery domain only.
- `SuccessAck`/`FailureAck` close the processing domain.
- If `AcceptedAck` is lost, the delivery retry may produce a duplicate. This is accepted at-least-once behavior.
- Acks should carry a correlation value (for example `attempt` or a per-delivery `deliveryId`) so a stale ack from an older publish cannot overwrite a newer attempt. The exact field is an open question.

## Backward Compatibility

- Existing listeners without `@RetryPolicy`: unchanged behavior; no local processing retry.
- Existing `FAILED` rows: remain readable; migration may backfill `event_retry` rows or fall back to the legacy scheduler heuristic during a transition period.
- Mixed fleets: a publisher that does not understand `AcceptedAck` treats it as an unknown technical event; a consumer that does not emit it falls back to ack-timeout delivery retry. This degrades to at-least-once but does not corrupt state.
- `EventStatus` values and their codes remain unchanged.

## Consequences

### Positive

- Delivery and processing failures become distinguishable using evidence, not guesses.
- A `publish()` exception no longer automatically causes a duplicate delivery if the consumer actually accepted the event.
- No `EventStatus` expansion; `FAILED` stays stable for existing dashboards and stores.
- Retry policy tuning becomes operational (database) without sacrificing explicit code-level policies.
- Dead-letter state and retry attempts are inspectable in one table, with a joinable overview view.

### Negative / Trade-offs

- Adds a new technical ack and a consumer-side emission point, increasing protocol surface.
- `AcceptedAck` can itself be lost, so duplicate delivery remains possible.
- The dispatcher must emit acceptance before handler execution; ack ordering and correlation need careful handling.
- Two new tables, a transactional repository, policy caching and claim/lease logic are non-trivial.
- `FAILED` remains ambiguous in dashboards unless they join `event_retry`.

## Open Questions

| # | Question | Current recommendation |
|---|---|---|
| 1 | Should `AcceptedAck` carry `attempt`, `deliveryId`, or both? | `deliveryId` for stale-ack protection; decide before protocol freeze |
| 2 | Is `AcceptedAck` emitted synchronously before handler execution, or asynchronously? | Emit before execution; log/metric if ack publication fails |
| 3 | Does `event_retry` need history, or one active row per event? | One active row per event for MVP; history via logs/metrics |
| 4 | Should `enabled = false` disable retries or fall back to the code policy? | Disable retries explicitly; make it visible in diagnostics |
| 5 | Are exception filters JSON or newline-separated text? | JSON array; avoid a child table for now |
| 6 | Is seeding from code enabled by default? | Phase 3 concern; opt-in when `event_retry_policy` exists |
| 7 | Should ack-timeout delivery tasks be pre-created or detected by a scheduler query? | Detect by scheduler query first, avoid a row per healthy publish |
| 8 | What status does an event have while processing after `AcceptedAck`? | Keep `PUBLISHED`; do not add a new status |
| 9 | Should `NoHandlerAck` be its own event or a flag on `AcceptedAck`? | Separate event; different semantics and metrics |
| 10 | How long are `DEAD` retry rows retained? | Separate, longer retention than `HANDLED`; value pending |

## Implementation Phases (Not Started)

**Phase 0 — design only (current):** finalize this ADR, the ack protocol and the `event_retry`/`event_retry_policy` schema. No production code changes.

**Phase 1 — acceptance boundary:** `AcceptedAck`, `NoHandlerAck`, dispatcher emission before handler execution, correlation and stale-ack handling.

**Phase 2 — retry queue:** `event_retry`, `LifecycleRepository`, claim/lease, `FAILED` as a common failure marker, delivery/processing routing. Policy resolution uses `@RetryPolicy` + properties/YAML overrides; no policy table.

**Phase 3 — policy storage (deferred):** `event_retry_policy` table, cache/fallback, optional seeding from code, runtime tuning without redeploy.

**Phase 4 — operations:** overview view, dead-letter retention, metrics, manual retry, documentation and migration notes.
