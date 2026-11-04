package io.github.vovten.eventflow.lifecycle.retry;

/**
 * State of a durable retry row.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public enum RetryState {

    /**
     * The retry is not claimed and becomes eligible when {@code nextAttemptAt} is reached.
     */
    PENDING,

    /**
     * A scheduler instance has claimed the retry and is waiting for the outcome ack.
     */
    IN_PROGRESS,

    /**
     * Terminal dead-letter state. No automatic retry remains.
     */
    DEAD
}
