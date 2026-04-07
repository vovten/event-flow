package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * Event publisher decorator that publishes events asynchronously.
 * <p>
 * This decorator wraps another publisher and submits the actual publishing work
 * to a provided {@link Executor}, returning immediately without blocking the caller.
 * This is useful for non-critical events where the caller should not wait for
 * network I/O or external system latency.
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Non-blocking — {@code publish()} returns immediately</li>
 *   <li>Fire-and-forget semantics — exceptions are caught and logged</li>
 *   <li>Caller-provided {@link Executor} — full control over thread pool lifecycle</li>
 *   <li>Compatible with other decorators (Retry, Transactional, Silent)</li>
 * </ul>
 * <p>
 * <b>When to use:</b>
 * <ul>
 *   <li>Analytics and tracking events</li>
 *   <li>Audit logs and metrics collection</li>
 *   <li>Non-critical notifications</li>
 *   <li>High-throughput scenarios where blocking is unacceptable</li>
 * </ul>
 * <p>
 * <b>When NOT to use:</b>
 * <ul>
 *   <li>Critical business events that must be confirmed delivered</li>
 *   <li>When the caller must handle publishing failures</li>
 *   <li>When event ordering guarantees are required</li>
 * </ul>
 * <p>
 * <b>Decorator ordering:</b>
 * {@code AsyncEventPublisher} should be the <b>outermost</b> decorator so that
 * retry logic, transaction synchronization, and silent error handling are all
 * executed within the async task, not on the caller's thread.
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * Executor executor = Executors.newCachedThreadPool();
 * EventPublisher asyncPublisher = new AsyncEventPublisher(
 *     new SilentEventPublisher(
 *         new ChannelEventPublisher(channels)
 *     ),
 *     executor
 * );
 * asyncPublisher.publish(new UserVisitedPageEvent());  // Returns immediately
 * }</pre>
 * <p>
 * <b>Combination with TransactionalEventPublisher:</b>
 * When used with Spring's {@code TransactionalEventPublisher}, the async decorator
 * must be outermost. The transaction's {@code afterCommit} callback will still
 * execute on the committing thread, but the actual publish call happens asynchronously.
 * <pre>{@code
 * EventPublisher publisher = new AsyncEventPublisher(
 *     new TransactionalEventPublisher(
 *         new ChannelEventPublisher(channels)
 *     ),
 *     executor
 * );
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-04-05
 * @see SilentEventPublisher
 * @see ChannelEventPublisher
 */
public class AsyncEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(AsyncEventPublisher.class);

    private final EventPublisher origin;
    private final Executor executor;

    /**
     * Create async publisher with the given executor.
     * <p>
     * Exceptions during publishing are caught and logged at WARN level.
     *
     * @param origin   the delegate publisher to wrap
     * @param executor the executor to use for async publishing
     * @throws IllegalArgumentException if origin or executor is null
     */
    public AsyncEventPublisher(EventPublisher origin, Executor executor) {
        if (origin == null) {
            throw new IllegalArgumentException("EventPublisher delegate must not be null");
        }
        if (executor == null) {
            throw new IllegalArgumentException("Executor must not be null");
        }
        this.origin = origin;
        this.executor = executor;
    }

    /**
     * Publish event asynchronously.
     * <p>
     * The actual publishing is submitted to the configured executor and this method
     * returns immediately. Any exceptions during publishing are caught and logged.
     *
     * @param event the event to publish
     */
    @Override
    public void publish(Event event) {
        String eventTypeName = event.type().getSimpleName();
        executor.execute(() -> {
            try {
                origin.publish(event);
                log.debug("Async event {} published successfully", eventTypeName);
            } catch (Exception e) {
                log.warn("Failed to async publish event '{}': {}", eventTypeName, e.getMessage(), e);
            }
        });
    }
}
