package io.github.vovten.eventflow.publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-event-type circuit breaker state machine.
 * <p>
 * Tracks publish failures for a specific event type and opens the circuit
 * when the failure rate exceeds a configured threshold. When open, subsequent
 * publish attempts are rejected immediately without calling the underlying
 * transport, giving the system time to recover.
 * <p>
 * <b>State transitions:</b>
 * <ul>
 *   <li>{@code CLOSED} — normal operation, requests pass through</li>
 *   <li>{@code OPEN} — failures detected, requests rejected immediately</li>
 *   <li>{@code HALF_OPEN} — after cooldown, limited attempts are allowed to
 *       probe if the system has recovered</li>
 * </ul>
 * <p>
 * <b>Thread safety:</b>
 * All state transitions use atomic operations and {@code compareAndSet} to
 * ensure correctness under concurrent access.
 * <p>
 * This class is package-private and used internally by {@link CircuitBreakerEventPublisher}.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
final class EventTypeCircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(EventTypeCircuitBreaker.class);

    /**
     * Circuit breaker states.
     */
    enum State {
        /** Normal operation — requests pass through to the transport. */
        CLOSED,
        /** Circuit is open — requests are rejected immediately. */
        OPEN,
        /** After cooldown — limited requests are allowed to test recovery. */
        HALF_OPEN
    }

    private final String eventType;
    private final Duration cooldown;
    private final int failureThreshold;
    private final int halfOpenMaxAttempts;
    private final double failureRateThreshold;

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private final AtomicReference<Instant> openedAt = new AtomicReference<>(null);
    private final AtomicInteger halfOpenAttempts = new AtomicInteger(0);

    /**
     * Creates a new circuit breaker for the given event type.
     *
     * @param eventType           the fully qualified event type name
     * @param failureThreshold    minimum number of requests before failure rate is evaluated
     * @param failureRateThreshold the failure rate threshold (0.0–1.0) that triggers opening
     * @param cooldown            duration to wait before transitioning from OPEN to HALF_OPEN
     * @param halfOpenMaxAttempts max failed attempts in HALF_OPEN before re-opening
     */
    EventTypeCircuitBreaker(String eventType, int failureThreshold, double failureRateThreshold,
                            Duration cooldown, int halfOpenMaxAttempts) {
        this.eventType = Objects.requireNonNull(eventType, "eventType must not be null");
        this.failureThreshold = failureThreshold;
        this.failureRateThreshold = failureRateThreshold;
        this.cooldown = Objects.requireNonNull(cooldown, "cooldown must not be null");
        this.halfOpenMaxAttempts = halfOpenMaxAttempts;
    }

    /**
     * Returns the current state.
     *
     * @return the current state
     */
    State state() {
        return state.get();
    }

    /**
     * Returns whether the circuit is currently open.
     * <p>
     * If the cooldown has elapsed since opening, this method may transition
     * the state from {@code OPEN} to {@code HALF_OPEN} atomically, allowing
     * the next request through.
     *
     * @return {@code true} if the circuit is open and requests should be rejected
     */
    boolean isOpen() {
        State s = state.get();
        if (s == State.OPEN) {
            Instant opened = openedAt.get();
            if (opened != null && opened.plus(cooldown).isBefore(Instant.now())) {
                if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                    halfOpenAttempts.set(0);
                    log.info("Circuit breaker transitioned from OPEN to HALF_OPEN for event type: {}", eventType);
                    return false;
                }
                // Another thread already transitioned
                return state.get() == State.OPEN;
            }
            return true;
        }
        return false;
    }

    /**
     * Records a successful publish.
     * <p>
     * In {@code HALF_OPEN} state, a single success closes the circuit.
     * In {@code CLOSED} state, increments the request count for rate calculation.
     */
    void onSuccess() {
        State s = state.get();
        if (s == State.HALF_OPEN) {
            // A single success in HALF_OPEN is enough to close
            if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                reset();
                log.info("Circuit breaker transitioned from HALF_OPEN to CLOSED for event type: {}", eventType);
            }
            return;
        }
        if (s == State.CLOSED) {
            requestCount.incrementAndGet();
        }
    }

    /**
     * Records a failed publish.
     * <p>
     * In {@code HALF_OPEN} state, may re-open the circuit if enough failures occur.
     * In {@code CLOSED} state, increments failure count and checks if the
     * failure rate threshold has been exceeded.
     */
    void onFailure() {
        State s = state.get();
        if (s == State.HALF_OPEN) {
            int attempts = halfOpenAttempts.incrementAndGet();
            if (attempts >= halfOpenMaxAttempts) {
                if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
                    openedAt.set(Instant.now());
                    halfOpenAttempts.set(0);
                    log.warn("Circuit breaker transitioned from HALF_OPEN to OPEN for event type: {} ({} failures in half-open)",
                            eventType, attempts);
                }
            }
            return;
        }
        if (s == State.CLOSED) {
            int failures = failureCount.incrementAndGet();
            int total = requestCount.incrementAndGet();

            if (total >= failureThreshold) {
                double rate = (double) failures / total;
                log.debug("Circuit breaker failure rate for {}: {}/{} = {}/{}",
                        eventType, failures, total, String.format("%.2f", rate), failureRateThreshold);
                if (rate >= failureRateThreshold) {
                    if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                        openedAt.set(Instant.now());
                        // Reset counters for the next window
                        failureCount.set(0);
                        requestCount.set(0);
                        log.warn("Circuit breaker transitioned from CLOSED to OPEN for event type: {} "
                                        + "(failure rate {}/{} = {} >= {})",
                                eventType, failures, total, String.format("%.2f", rate), failureRateThreshold);
                    }
                } else {
                    // Rate is below threshold — reset counters for the next window
                    failureCount.set(0);
                    requestCount.set(0);
                }
            }
        }
    }

    /**
     * Records a failure without incrementing the request count.
     * <p>
     * Used for handler-side failures (e.g., {@code FailureAck}) where the
     * request has already been counted during the original publish.
     * Only increments the failure count — the request count stays unchanged.
     * <p>
     * In {@code HALF_OPEN} state, behaves identically to {@link #onFailure()}.
     */
    void onFailureWithoutRequest() {
        State s = state.get();
        if (s == State.HALF_OPEN) {
            onFailure();
            return;
        }
        if (s == State.CLOSED) {
            int failures = failureCount.incrementAndGet();
            int total = requestCount.get();

            if (total >= failureThreshold) {
                double rate = (double) failures / total;
                log.debug("Circuit breaker failure rate for {} (ack): {}/{} = {}/{}",
                        eventType, failures, total, String.format("%.2f", rate), failureRateThreshold);
                if (rate >= failureRateThreshold) {
                    if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                        openedAt.set(Instant.now());
                        failureCount.set(0);
                        requestCount.set(0);
                        log.warn("Circuit breaker transitioned from CLOSED to OPEN for event type: {} "
                                        + "(ack failure rate {}/{} = {} >= {})",
                                eventType, failures, total, String.format("%.2f", rate), failureRateThreshold);
                    }
                } else {
                    failureCount.set(0);
                    requestCount.set(0);
                }
            }
        }
    }

    /**
     * Resets all counters and timestamps to initial state.
     */
    void reset() {
        state.set(State.CLOSED);
        failureCount.set(0);
        requestCount.set(0);
        openedAt.set(null);
        halfOpenAttempts.set(0);
    }
}
