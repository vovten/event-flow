package io.github.vovten.eventflow.lifecycle;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the retry-aware {@link EventLifecyclePublisher}.
 *
 * @param requireAcceptedAck when {@code true}, a successful publish moves the event to
 *                           {@code PUBLISHED} and waits for an {@code AcceptedAck};
 *                           when {@code false}, publish success is treated as
 *                           {@code ACCEPTED} (legacy mixed-fleet behaviour)
 * @param deliveryRetryDelay base delay before the first durable delivery retry
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public record EventLifecyclePublisherOptions(
        boolean requireAcceptedAck,
        Duration deliveryRetryDelay
) {

    public EventLifecyclePublisherOptions {
        Objects.requireNonNull(deliveryRetryDelay, "deliveryRetryDelay must not be null");
        if (deliveryRetryDelay.isNegative()) {
            throw new IllegalArgumentException("deliveryRetryDelay must not be negative");
        }
    }
}
