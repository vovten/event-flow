package io.github.vovten.eventflow.lifecycle.store;

import java.util.Arrays;

/**
 * Status of an event in the event store lifecycle.
 * <p>
 * Flow:
 * <pre>
 * PERSISTED:                         UNDEFINED (terminal, no tracking)
 *
 * MANAGED (full lifecycle):
 *   NEW ──publish──► PUBLISHED ──ack──► HANDLED
 *    │                                    │
 *    └──failed──► FAILED ──retry──► NEW   │
 *                                         │
 *                    (ack-failed) ────────┘──► FAILED ──retry──► NEW
 * </pre>
 * <p>
 * Each status has a numeric code used for efficient storage in the database.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public enum EventStatus {

    /** Event was persisted but is not lifecycle-tracked (PERSISTED lifecycle). */
    UNDEFINED(0),

    /** Event saved and lifecycle-tracked (MANAGED lifecycle), not yet published successfully. */
    NEW(1),

    /** Event was successfully published to all target transports. */
    PUBLISHED(2),

    /** Event was successfully handled by all registered handlers. Set via ack from dispatcher. */
    HANDLED(3),

    /**
     * Event publication or handling failed. Eligible for retry.
     * Check {@link StoredEvent#errorDetails()} for the specific cause.
     */
    FAILED(4);

    private final int code;

    EventStatus(int code) {
        this.code = code;
    }

    /**
     * Returns the numeric code for this status.
     *
     * @return status code
     */
    public int getCode() {
        return code;
    }

    /**
     * Resolves a status from its numeric code.
     *
     * @param code the numeric code
     * @return the corresponding status
     * @throws IllegalArgumentException if no status matches the code
     */
    public static EventStatus fromCode(int code) {
        return Arrays.stream(values())
                .filter(s -> s.code == code)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown status code: " + code));
    }
}
