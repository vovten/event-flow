package io.github.vovten.eventflow.publisher;

/**
 * Tracks failures for circuit breaker decisions per event type.
 * <p>
 * This interface allows lifecycle components (such as {@code AckHandler}) to
 * signal handler-side failures to the circuit breaker without creating a
 * circular dependency (lifecycle → publisher).
 * <p>
 * The circuit breaker is optional — when not configured, a no-op implementation
 * can be used.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 * @see CircuitBreakerEventPublisher
 */
public interface FailureTracker {

    /**
     * Records a failure for the given event type.
     * <p>
     * The event type should match the key used by
     * {@link CircuitBreakerEventPublisher} for grouping — for {@code Envelope}
     * events this is the payload class name, for other events it is the
     * fully qualified class name from {@code event.type().getName()}.
     *
     * @param eventType the fully qualified event type name that failed
     */
    void recordFailure(String eventType);
}
