package io.github.vovten.eventflow.dispatcher;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.transport.AsyncEventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Event dispatcher decorator that ensures idempotent event processing using a Caffeine cache.
 * <p>
 * This dispatcher wraps another {@link EventDispatcher} and prevents duplicate processing
 * of events that have already been handled. It relies on {@link TraceableEvent} to provide
 * unique identifiers (UID) for event deduplication.
 * <p>
 * <b>Decorator chain support:</b>
 * <pre>{@code
 * IdempotentEventDispatcher.start(dispatcher::dispatch) → origin.start(this::dispatch)
 * }</pre>
 * This ensures events flow through the decorator before reaching the origin dispatcher.
 *
 * <p>
 * <b>Limitations.</b> Deduplication is an in-memory, per-instance Caffeine cache:
 * <ul>
 *   <li>entries expire after the configured TTL (10 minutes by default), so a
 *       redelivery after the TTL is processed again;</li>
 *   <li>a JVM restart loses the cache — replayed messages are treated as new;</li>
 *   <li>each consumer instance has its own cache, so a cluster can process the
 *       same delivery on two instances after a rebalance.</li>
 * </ul>
 * These are accepted at-least-once semantics; listeners must be idempotent
 * (for example deduplicate by {@code eventId}/{@code processId} on their side).
 * The cache is invalidated when a dispatch is not fully successful, so a retry
 * of a partially/fully failed event is re-executed.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public final class IdempotentEventDispatcher implements EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(IdempotentEventDispatcher.class);

    private final EventDispatcher origin;
    private final Cache<UUID, CompletableFuture<HandlerResults>> cache;
    private final boolean warnOnDuplicate;

    public IdempotentEventDispatcher(EventDispatcher origin,
                              Duration ttl,
                              long maxSize,
                              boolean warnOnDuplicate) {
        this.origin = origin;
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(ttl)
                .maximumSize(maxSize)
                .build();
        this.warnOnDuplicate = warnOnDuplicate;
    }

    @Override
    public void start(AsyncEventConsumer dispatchConsumer) {
        origin.start(dispatchConsumer);
    }

    @Override
    public void stop() {
        origin.stop();
        log.info("Cache stats: {}", cache.stats());
    }

    @Override
    public CompletableFuture<HandlerResults> dispatch(Event event) {
        if (!(event instanceof TraceableEvent traceable)) {
            return origin.dispatch(event);
        }
        UUID eventId = traceable.eventId();
        if (eventId == null) {
            return origin.dispatch(event);
        }
        CompletableFuture<HandlerResults> result = new CompletableFuture<>();
        CompletableFuture<HandlerResults> previous = cache.asMap().putIfAbsent(eventId, result);
        if (previous != null) {
            if (warnOnDuplicate) {
                log.warn("Duplicate event ignored: {}", eventId);
            }
            // Share the original outcome: a successful in-flight original is a
            // duplicate no-op, while an original failure must fail this delivery
            // too so the transport does not commit its offset.
            return duplicateView(previous);
        }
        try {
            origin.dispatch(event).whenComplete((results, throwable) -> {
                if (throwable != null) {
                    cache.invalidate(eventId);
                    result.completeExceptionally(throwable);
                    log.debug("Event dispatch failed, removed from cache: {}", eventId);
                } else if (results == null || !results.isAllSuccess()) {
                    cache.invalidate(eventId);
                    result.complete(results);
                    log.debug("Event was not fully processed, removed from cache: {}", eventId);
                } else {
                    result.complete(results);
                    log.debug("Event processed: {}", eventId);
                }
            });
        } catch (RuntimeException e) {
            cache.invalidate(eventId);
            result.completeExceptionally(e);
        }
        return result;
    }

    /**
     * Creates the view returned to an in-flight duplicate.
     * <p>
     * The duplicate must observe the original failure (or exception) so the
     * transport keeps the offset uncommitted. A successful original is mapped
     * to {@link HandlerResults#duplicate()} to preserve the existing duplicate
     * log/contract.
     *
     * @param original the future of the original in-flight delivery
     * @return future completed with the duplicate view of the original outcome
     */
    private CompletableFuture<HandlerResults> duplicateView(
            CompletableFuture<HandlerResults> original) {
        return original.thenApply(results -> {
            if (results != null && results.isAllSuccess()) {
                return HandlerResults.duplicate();
            }
            return results;
        });
    }

    @Override
    public long maxProcessingMs(Event event) {
        return origin.maxProcessingMs(event);
    }

    @Override
    public void register(Object handler) {
        origin.register(handler);
    }

    @Override
    public boolean isRegistered(Object handler) {
        return origin.isRegistered(handler);
    }
}