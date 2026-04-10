package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Event publisher decorator that silently catches and logs all publishing errors.
 * <p>
 * This publisher wraps another publisher and catches any exceptions that occur during
 * event publishing. Instead of propagating the exception, it logs the error and returns
 * a result list that includes a failed {@link SendResult} entry. This allows the caller
 * to inspect failures without handling exceptions.
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Never completes exceptionally — all errors are logged</li>
 *   <li>Returns failed SendResult entries instead of throwing</li>
 *   <li>Preserves partial results when some transports succeed</li>
 *   <li>Configurable log level (WARN or DEBUG)</li>
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
 * silentPublisher.publish(new UserClickedButtonEvent())
 *     .thenAccept(results -> {
 *         for (SendResult result : results) {
 *             if (!result.success()) {
 *                 log.warn("Failed to send: {}", result.errorDetails());
 *             }
 *         }
 *     });
 * }</pre>
 * <p>
 * <b>Combination with RetryEventPublisher:</b>
 * <pre>{@code
 * // Retry 3 times, then silently return failed results if still failing
 * EventPublisher publisher = new SilentEventPublisher(
 *     new RetryEventPublisher(
 *         new ChannelEventPublisher(channels),
 *         3, Duration.ofMillis(100), 2.0
 *     )
 * );
 * }</pre>
 * <p>
 * <b>Note:</b> For transactional event publishing with silent error handling, use the
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
     * Instead, a failed {@link SendResult} entry is added to the result list,
     * allowing the caller to inspect failures without handling exceptions.
     *
     * @param event the event to publish
     * @return CompletableFuture that completes with SendResults (never exceptionally)
     */
    @Override
    public CompletableFuture<List<SendResult>> publish(Event event) {
        return origin.publish(event)
                .handle((results, ex) -> {
                    if (ex != null) {
                        Throwable cause = unwrap(ex);
                        String causeMsg = cause.getMessage();
                        String typeName = event.type().getSimpleName();
                        String msg = "Failed to publish event '{}' (silently ignored): {}";
                        if (logWarnings) {
                            log.warn(msg, typeName, causeMsg, cause);
                        } else {
                            log.debug(msg, typeName, causeMsg, cause);
                        }
                        List<SendResult> newResults = new ArrayList<>();
                        if (results != null) {
                            newResults.addAll(results);
                        }
                        newResults.add(SendResult.failure("publisher", cause, causeMsg));
                        return newResults;
                    }
                    return results;
                });
    }

    /**
     * Unwrap CompletionException to get the real cause.
     *
     * @param ex the exception to unwrap
     * @return the root cause exception
     */
    private Throwable unwrap(Throwable ex) {
        if (ex instanceof CompletionException && ex.getCause() != null) {
            return ex.getCause();
        }
        return ex;
    }
}
