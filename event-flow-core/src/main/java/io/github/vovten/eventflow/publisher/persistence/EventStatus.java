package io.github.vovten.eventflow.publisher.persistence;

/**
 * Status of a persisted event in the outbox table.
 * <p>
 * Database mapping (SMALLINT):
 * <ul>
 *   <li>0 = PENDING - Event is saved but not yet published</li>
 *   <li>1 = PUBLISHED - Event was successfully published</li>
 *   <li>2 = FAILED - Event publishing failed after all retries</li>
 * </ul>
 */
public enum EventStatus {
    /** Event is saved but not yet published */
    PENDING(0),
    /** Event was successfully published */
    PUBLISHED(1),
    /** Event publishing failed after all retries */
    FAILED(2);

    private final int code;

    EventStatus(int code) {
        this.code = code;
    }

    /**
     * @return numeric code for database storage
     */
    public int code() {
        return code;
    }

    /**
     * Find EventStatus by numeric code.
     *
     * @param code numeric code (0, 1, 2)
     * @return corresponding EventStatus
     * @throws IllegalArgumentException if code is invalid
     */
    public static EventStatus fromCode(int code) {
        for (EventStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown status code: " + code);
    }
}