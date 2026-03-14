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

    private final EventDispatcher origin;
    private final Cache<UUID, Boolean> cache;
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
    public void start() {
        origin.start();
    }

    @Override
    public void stop() {
        origin.stop();
        log.info("Cache stats: {}", cache.stats());
    }

    @Override
    public void dispatch(Event event) {
        if (!(event instanceof TraceableEvent traceable)) {
            origin.dispatch(event);
            return;
        }
        UUID uid = traceable.uid();
        Boolean existing = cache.getIfPresent(uid);

        if (existing == null) {
            origin.dispatch(event);
            cache.put(uid, Boolean.TRUE);
            log.debug("Event processed: {}", uid);
        } else {
            if (warnOnDuplicate) {
                log.warn("Duplicate event ignored: {}", uid);
            }
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