package com.github.vovten.eventflow.dispatcher;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.event.TraceableEvent;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.UUID;

/**
 * Event dispatcher decorator that ensures idempotent event processing using a Caffeine cache.
 * <p>
 * This dispatcher wraps another {@link EventDispatcher} and prevents duplicate processing
 * of events that have already been handled. It relies on {@link TraceableEvent} to provide
 * unique identifiers (UID) for event deduplication.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-13
 */
@Slf4j
public class IdempotentEventDispatcher implements EventDispatcher {

    private final EventDispatcher delegate;
    private final Cache<UUID, Boolean> cache;
    private final boolean warnOnDuplicate;

    public IdempotentEventDispatcher(EventDispatcher delegate,
                                     Duration ttl,
                                     long maxSize,
                                     boolean warnOnDuplicate) {
        this.delegate = delegate;
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(ttl)
                .maximumSize(maxSize)
                .build();
        this.warnOnDuplicate = warnOnDuplicate;
    }

    @Override
    public void start() {
        delegate.start();
    }

    @Override
    public void stop() {
        delegate.stop();
        log.info("Cache stats: {}", cache.stats());
    }

    @Override
    public void dispatch(Event event) {
        if (!(event instanceof TraceableEvent traceable)) {
            delegate.dispatch(event);
            return;
        }
        UUID uid = traceable.uid();

        // put if absent - returns null if first time
        Boolean existing = cache.get(uid, k -> {
            delegate.dispatch(event);
            return Boolean.TRUE;
        });

        if (existing != null && warnOnDuplicate) {
            log.warn("Duplicate event ignored: {}", uid);
        }
    }

    @Override
    public void register(Object handler) {
        delegate.register(handler);
    }

    @Override
    public boolean isRegistered(Object handler) {
        return delegate.isRegistered(handler);
    }
}