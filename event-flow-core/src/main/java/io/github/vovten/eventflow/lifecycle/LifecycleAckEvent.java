package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.event.Event;

import java.util.UUID;

/**
 * Interface for lifecycle acknowledgment events.
 * <p>
 * These events are published by the {@code EventLifecycleDispatcher} back to the
 * source channel after handlers have processed the original event.
 * <p>
 * On the publisher side, {@code PersistentEventPublisher} skips storing
 * {@code LifecycleAckEvent} instances (they are technical, not business events).
 * Similarly, {@code EventLifecycleDispatcher} skips publishing acks for
 * incoming {@code LifecycleAckEvent} instances to avoid infinite loops.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public interface LifecycleAckEvent extends Event {

    /**
     * Returns the ID of the original event that this ack refers to.
     *
     * @return original event ID
     */
    UUID originalEventId();
}
