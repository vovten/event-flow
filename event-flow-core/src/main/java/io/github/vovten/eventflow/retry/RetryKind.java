package io.github.vovten.eventflow.retry;

/**
 * Retry domain discriminator.
 * <p>
 * The event lifecycle distinguishes two independent retry domains:
 * <ul>
 *   <li>{@link #DELIVERY} — publisher-to-consumer delivery was not confirmed
 *       by an {@code AcceptedAck}; the event must be delivered again;</li>
 *   <li>{@link #PROCESSING} — the consumer accepted the event, but one or more
 *       handlers failed; the event must be processed again.</li>
 * </ul>
 * <p>
 * The two domains are scheduled, counted and stored independently. A single
 * event may have both a delivery and a processing retry pending at the same
 * time.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public enum RetryKind {

    /**
     * The event has not been confirmed as delivered to a consumer.
     * Delivery is confirmed by an {@code AcceptedAck}.
     */
    DELIVERY,

    /**
     * The event was delivered, but handler processing failed and must be retried.
     * Processing is confirmed by a {@code SuccessAck}.
     */
    PROCESSING
}
