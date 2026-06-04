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
 * Each status has a single-character code used for efficient storage in the database.
 * The codes are mnemonic first letters of the status names for readability in DB dumps.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public enum EventStatus {

    /**
     * Event was persisted but is not lifecycle-tracked (PERSISTED lifecycle).
     */
    UNDEFINED('U'),

    /**
     * Event saved and lifecycle-tracked (MANAGED lifecycle), not yet published successfully.
     */
    NEW('N'),

    /**
     * Event was successfully published to all target transports.
     */
    PUBLISHED('P'),

    /**
     * Event was successfully handled by all registered handlers. Set via ack from dispatcher.
     */
    HANDLED('H'),

    /**
     * Event publication or handling failed. Eligible for retry.
     * Check {@link StoredEvent#errorDetails()} for the specific cause.
     */
    FAILED('F');

    private final char code;

    EventStatus(char code) {
        this.code = code;
    }

    /**
     * Returns the character code for this status.
     *
     * @return status code character
     */
    public char getCode() {
        return code;
    }

    /**
     * Resolves a status from its character code.
     *
     * @param code the character code (e.g., 'U', 'N', 'P', 'H', 'F')
     * @return the corresponding status
     * @throws IllegalArgumentException if no status matches the code
     */
    public static EventStatus fromCode(char code) {
        return Arrays.stream(values())
                .filter(s -> s.code == code)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown status code: " + code));
    }
}
