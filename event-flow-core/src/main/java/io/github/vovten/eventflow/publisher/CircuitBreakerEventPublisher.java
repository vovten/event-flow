package io.github.vovten.eventflow.publisher;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Circuit breaker decorator for {@link EventPublisher} that protects the transport
 * layer from cascading failures.
 * <p>
 * When a configurable failure rate is exceeded for a specific event type, the
 * circuit opens and subsequent publish attempts for that event type are rejected
 * immediately without calling the underlying transport. After a cooldown period,
 * the circuit transitions to half-open state and allows limited attempts to
 * probe if the system has recovered.
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Per-event-type circuit breaker — one event type failing doesn't affect others</li>
 *   <li>Thread-safe — uses {@link com.github.benmanes.caffeine.cache.Caffeine} cache
 *       and atomic state transitions; idle CLOSED entries are evicted automatically</li>
 *   <li>Bypass mechanism — {@link #runWithBypass(Runnable)} allows scheduler retries
 *       to bypass the breaker without affecting breaker state</li>
 *   <li>No event loss — events are persisted before the breaker is checked
 *       (when combined with {@link io.github.vovten.eventflow.lifecycle.EventLifecyclePublisher})</li>
 * </ul>
 * <p>
 * <b>State machine:</b>
 * <pre>{@code
 *         ┌──────────┐
 *         │  CLOSED  │ ── failure rate > threshold ──→ ┌──────┐
 *         └──────────┘                                 │ OPEN │
 *              ↑                                       └──────┘
 *              │              cooldown elapsed             │
 *              │         ┌────────────┐                    │
 *              └─────────│ HALF_OPEN  │ ◄──────────────────┘
 *                        └────────────┘
 *                         success → CLOSED
 *                         failure ≥ halfOpenMaxAttempts → OPEN
 * }</pre>
 * <p>
 * <b>Bypass mechanism:</b>
 * The {@link #runWithBypass(Runnable)} method allows controlled bypass of the
 * circuit breaker. This is used by the scheduler to ensure manual retries and
 * automatic retry cycles are not blocked by an open circuit:
 * <pre>{@code
 * CircuitBreakerEventPublisher.runWithBypass(() -> publisher.publish(event));
 * }</pre>
 * Bypassed requests do NOT count toward the breaker's failure rate.
 * <p>
 * <b>When to use:</b>
 * <ul>
 *   <li>External service dependencies (HTTP APIs, databases, message brokers)</li>
 *   <li>Protecting resources from cascading failures during outages</li>
 *   <li>Reducing load on failing systems during recovery</li>
 * </ul>
 * <p>
 * <b>Integration with lifecycle tracking:</b>
 * This decorator wraps the transport publisher chain inside
 * {@link io.github.vovten.eventflow.lifecycle.EventLifecyclePublisher}. Events are
 * persisted before the breaker check, so no events are lost when the circuit is open.
 * The event store will contain a record with FAILED status, and the scheduler will
 * retry it later.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 * @see EventTypeCircuitBreaker
 * @see EventPublisher
 */
public final class CircuitBreakerEventPublisher implements EventPublisher, FailureTracker {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerEventPublisher.class);

    private static final ThreadLocal<Boolean> BYPASS = ThreadLocal.withInitial(() -> false);

    private final EventPublisher origin;
    private final Cache<String, EventTypeCircuitBreaker> breakers;
    private final int failureThreshold;
    private final double failureRateThreshold;
    private final Duration cooldown;
    private final int halfOpenMaxAttempts;
    private final int maxCacheSize;

    /**
     * Creates a new circuit breaker event publisher.
     *
     * @param origin               the delegate publisher to wrap
     * @param failureThreshold     minimum number of requests before evaluating failure rate
     * @param failureRateThreshold the failure rate (0.0–1.0) that triggers opening
     * @param cooldown             time to wait before transitioning from OPEN to HALF_OPEN
     * @param halfOpenMaxAttempts  max failed attempts in HALF_OPEN before re-opening
     * @param maxCacheSize         maximum number of circuit breaker entries in the cache
     */
    public CircuitBreakerEventPublisher(EventPublisher origin, int failureThreshold,
                                        double failureRateThreshold, Duration cooldown,
                                        int halfOpenMaxAttempts, int maxCacheSize) {
        this.origin = Objects.requireNonNull(origin, "origin must not be null");
        this.failureThreshold = failureThreshold;
        this.failureRateThreshold = failureRateThreshold;
        this.cooldown = Objects.requireNonNull(cooldown, "cooldown must not be null");
        this.halfOpenMaxAttempts = halfOpenMaxAttempts;
        this.maxCacheSize = maxCacheSize;
        this.breakers = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .build();
    }

    /**
     * Runs the given action with the circuit breaker bypassed.
     * <p>
     * Requests made within this context pass through the breaker regardless
     * of its state and do not affect the breaker's failure counters.
     * This is used by the retry scheduler to ensure manual retries are
     * not blocked by an open circuit.
     * <p>
     * Supports nested calls — the previous bypass state is restored
     * when the inner bypass completes.
     *
     * @param action the action to run with bypass
     */
    public static void runWithBypass(Runnable action) {
        Objects.requireNonNull(action, "action must not be null");
        boolean previous = BYPASS.get();
        BYPASS.set(true);
        try {
            action.run();
        } finally {
            BYPASS.set(previous);
        }
    }

    @Override
    public CompletableFuture<SendResults> publish(Event event) {
        Objects.requireNonNull(event, "event must not be null");

        // When bypass is active, pass through without any breaker logic
        if (BYPASS.get()) {
            return origin.publish(event);
        }

        String eventType = resolveEventType(event);
        EventTypeCircuitBreaker breaker = breakers.get(eventType, k ->
                new EventTypeCircuitBreaker(k, failureThreshold, failureRateThreshold,
                        cooldown, halfOpenMaxAttempts));

        // Check circuit state — may transition from OPEN to HALF_OPEN internally
        if (breaker.isOpen()) {
            log.warn("Circuit breaker OPEN for event type: {}, rejecting publish of event: {}",
                    eventType, event);
            return CompletableFuture.completedFuture(createRejectedResult(eventType));
        }

        return origin.publish(event)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.debug("Circuit breaker recording failure for event type: {} (error: {})",
                                eventType, error.getMessage());
                        breaker.onFailure();
                    } else if (result == null || result.isAllFailure()) {
                        String reason = result != null ? result.getSummary() : "null result";
                        log.debug("Circuit breaker recording failure for event type: {} (result: {})",
                                eventType, reason);
                        breaker.onFailure();
                    } else {
                        breaker.onSuccess();
                    }
                });
    }

    @Override
    public void recordFailure(String eventType) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        EventTypeCircuitBreaker breaker = breakers.get(eventType, k ->
                new EventTypeCircuitBreaker(k, failureThreshold, failureRateThreshold,
                        cooldown, halfOpenMaxAttempts));
        breaker.onFailureWithoutRequest();
    }

    /**
     * Resolves the event type name used for circuit breaker grouping.
     * <p>
     * For {@link Envelope} events, uses the payload class name.
     * For other events, uses the event's type name.
     *
     * @param event the event to resolve
     * @return fully qualified class name for grouping
     */
    private static String resolveEventType(Event event) {
        if (event instanceof Envelope<?> envelope) {
            return envelope.payload().getClass().getName();
        }
        return event.type().getName();
    }

    /**
     * Creates a SendResults representing a rejected publish for logging/status tracking.
     *
     * @param eventType the event type that was rejected
     * @return SendResults with a single failure result
     */
    private static SendResults createRejectedResult(String eventType) {
        String msg = "Circuit breaker is OPEN for event type: " + eventType;
        return SendResults.of(List.of(
                SendResult.failure("circuit-breaker", new RuntimeException(msg), msg)
        ));
    }
}
