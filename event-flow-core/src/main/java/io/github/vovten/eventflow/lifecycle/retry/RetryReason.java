package io.github.vovten.eventflow.lifecycle.retry;

/**
 * Reason why a durable retry row was created or updated.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public enum RetryReason {

    /**
     * The publish call failed before delivery was confirmed.
     */
    PUBLISH_FAILED,

    /**
     * At least one destination failed while other destinations succeeded.
     */
    PARTIAL_PUBLISH_FAILED,

    /**
     * No acceptance ack arrived within the delivery timeout.
     */
    ACK_TIMEOUT,

    /**
     * A handler failed and the local retry budget was exhausted.
     */
    HANDLER_FAILED,

    /**
     * The consumer accepted the event but no final ack arrived within the timeout.
     */
    PROCESSING_TIMEOUT,

    /**
     * No listener was resolved for the event type.
     */
    NO_HANDLER,

    /**
     * The configured retry budget was exhausted.
     */
    CAP_EXCEEDED
}
