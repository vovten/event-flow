package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event publisher decorator that silently catches and logs all publishing errors.
 * <p>
 * This publisher wraps another publisher and catches any exceptions that occur during
 * event publishing. Instead of propagating the exception, it logs the error and continues.
 * This is useful for "fire-and-forget" scenarios where event delivery is not critical.
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Never throws exceptions — all errors are logged</li>
 *   <li>Configurable log level (WARN or DEBUG)</li>
 *   <li>Detailed error context (event type, error message, stack trace)</li>
 *   <li>Compatible with other decorators (Retry)</li>
 * </ul>
 * <p>
 * <b>When to use:</b>
 * <ul>
 *   <li>Analytics events — nice to have, but not critical</li>
 *   <li>High-throughput systems — prefer availability over guaranteed delivery</li>
 *   <li>Graceful degradation — continue working even if event bus is down</li>
 *   <li>Background events — user experience should not be affected</li>
 * </ul>
 * <p>
 * <b>When NOT to use:</b>
 * <ul>
 *   <li>Critical business events (orders, payments, security)</li>
 *   <li>When you need to guarantee event delivery</li>
 *   <li>When the caller must know about publishing failures</li>
 * </ul>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * // Fire-and-forget for analytics events
 * EventPublisher silentPublisher = new SilentEventPublisher(
 *     new ChannelEventPublisher(channels)
 * );
 * silentPublisher.publish(new UserClickedButtonEvent());  // Never throws
 * }</pre>
 * <p>
 * <b>Combination with RetryEventPublisher:</b>
 * <pre>{@code
 * // Retry 3 times, then silently ignore if still failing
 * EventPublisher publisher = new SilentEventPublisher(
 *     new RetryEventPublisher(
 *         new ChannelEventPublisher(channels),
 *         3, Duration.ofMillis(100), 2.0
 *     )
 * );
 * }</pre>
 * <p>
 * <b>Note:</b> For transactional publishing with silent error handling, use the
 * event-flow-spring module which provides Spring-aware transaction support.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-05
 * @see RetryEventPublisher
 * @see ChannelEventPublisher
 */
public class SilentEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(SilentEventPublisher.class);

    private final EventPublisher origin;
    private final boolean logWarnings;

    /**
     * Create silent publisher with default WARN level logging.
     *
     * @param origin the delegate publisher to wrap
     * @throws IllegalArgumentException if delegate is null
     */
    public SilentEventPublisher(EventPublisher origin) {
        this(origin, true);
    }

    /**
     * Create silent publisher with custom log level.
     *
     * @param origin the delegate publisher to wrap
     * @param logWarnings if true, log at WARN level; if false, log at DEBUG level
     * @throws IllegalArgumentException if delegate is null
     */
    public SilentEventPublisher(EventPublisher origin, boolean logWarnings) {
        if (origin == null) {
            throw new IllegalArgumentException("EventPublisher delegate must not be null");
        }
        this.origin = origin;
        this.logWarnings = logWarnings;
    }

    /**
     * Publish event silently.
     * <p>
     * Any exceptions thrown by the delegate publisher are caught and logged.
     * The exception is never propagated to the caller.
     *
     * @param event the event to publish
     */
    @Override
    public void publish(Event event) {
        try {
            origin.publish(event);
        } catch (Exception e) {
            String msg = "Failed to publish event '{}' (silently ignored): {}";
            if (logWarnings) {
                log.warn(msg, event.type().getSimpleName(), e.getMessage(), e);
            } else {
                log.debug(msg, event.type().getSimpleName(), e.getMessage(), e);
            }
        }
    }
}
