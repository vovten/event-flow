package io.github.vovten.eventflow.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Interface for events that can be traced through the system.
 * Provides unique identifier and timestamp for tracking, debugging,
 * and establishing event chronology.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-13
 */
public interface TraceableEvent extends Event {

    /**
     * Returns the unique identifier of the event.
     *
     * @return the unique event identifier
     */
    UUID eventId();

    /**
     * @return correlation ID that groups related events together (e.g., business process ID)
     */
    UUID traceId();

    /**
     * Returns the timestamp when the event occurred.
     *
     * @return the event occurrence timestamp
     */
    Instant occurredAt();
}