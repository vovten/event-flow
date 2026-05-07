package io.github.vovten.eventflow.publisher.persistence;

import java.time.Instant;
import java.util.UUID;

/**
 * Status of a persisted event.
 */
public enum EventStatus {
    /** Event is saved but not yet published */
    PENDING,
    /** Event was successfully published */
    PUBLISHED,
    /** Event publishing failed after all retries */
    FAILED
}
