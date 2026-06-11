package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;

/**
 * Resolves the {@link EventLifecycle} level for a given event.
 * <p>
 * Strategy-interface that encapsulates lifecycle resolution logic.
 * </p>
 * <p>
 * Standard resolution order (via {@link #standard()}):
 * <ol>
 *   <li>{@link io.github.vovten.eventflow.event.annotation.Event @Event} annotation on the event class
 *       (or on the payload class for {@link Envelope})</li>
 *   <li>{@link Event#lifecycle()} default method on the event interface</li>
 *   <li>{@link EventLifecycle#PERSISTED} as fallback for POJO payloads</li>
 * </ol>
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
@FunctionalInterface
public interface LifecycleResolver {

    /**
     * Resolves the lifecycle level for the given event.
     *
     * @param event the event to resolve lifecycle for (must not be null)
     * @return the resolved lifecycle level (never null)
     */
    EventLifecycle resolve(Event event);

    /**
     * Returns the standard lifecycle resolver that checks the
     * {@link io.github.vovten.eventflow.event.annotation.Event @Event} annotation first, then falls back to
     * {@link Event#lifecycle()}, and finally to {@link EventLifecycle#PERSISTED}.
     *
     * @return standard lifecycle resolver
     */
    static LifecycleResolver standard() {
        return event -> {
            if (event instanceof Envelope<?> envelope) {
                return resolveEnvelopeLifecycle(envelope);
            }
            var ann = event.getClass().getAnnotation(io.github.vovten.eventflow.event.annotation.Event.class);
            if (ann != null) {
                return ann.lifecycle();
            }
            return event.lifecycle();
        };
    }

    /**
     * Resolves lifecycle for an {@link Envelope}.
     * Priority: annotation on payload → payload.lifecycle() → PERSISTED.
     *
     * @param envelope the envelope to resolve lifecycle for
     * @return the resolved lifecycle level
     */
    private static EventLifecycle resolveEnvelopeLifecycle(Envelope<?> envelope) {
        Object payload = envelope.payload();
        var ann = payload.getClass().getAnnotation(io.github.vovten.eventflow.event.annotation.Event.class);
        if (ann != null) {
            return ann.lifecycle();
        }
        if (payload instanceof Event evt) {
            return evt.lifecycle();
        }
        return EventLifecycle.PERSISTED;
    }
}
