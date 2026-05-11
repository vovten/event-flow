package io.github.vovten.eventflow.dispatcher;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;

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
 * @author Vladimir Aleshkov
 * @since 2026-03-13
 */
public final class IdempotentEventDispatcher implements EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(IdempotentEventDispatcher.class);

    private final EventDispatcher origin;
    private final Cache<UUID, Boolean> cache;
    private final boolean warnOnDuplicate;

    IdempotentEventDispatcher(EventDispatcher origin,
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
    public void start(Consumer<Event> dispatchConsumer) {
        origin.start(this::dispatch);
    }

    @Override
    public void stop() {
        origin.stop();
        log.info("Cache stats: {}", cache.stats());
    }

    @Override
    public DispatchResult dispatch(Event event) {
        if (!(event instanceof TraceableEvent traceable)) {
            return origin.dispatch(event);
        }
        UUID eventId = traceable.eventId();
        Boolean existing = cache.getIfPresent(eventId);

        if (existing == null) {
            DispatchResult result = origin.dispatch(event);
            cache.put(eventId, Boolean.TRUE);
            log.debug("Event processed: {}", eventId);
            return result;
        } else {
            if (warnOnDuplicate) {
                log.warn("Duplicate event ignored: {}", eventId);
            }
            return new DispatchResult(0, 0);
        }
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