package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;

/**
 * Validates the two-domain retry configuration before any retry component is
 * created.
 * <p>
 * A durable processing retry depends on the delivery boundary: without delivery
 * tracking, a processing retry would silently become a delivery retry and the
 * separation between the domains would be lost.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class RetryConfigurationValidator {

    /**
     * Validates the retry configuration.
     *
     * @param properties event-flow properties
     * @throws IllegalStateException if the configuration is inconsistent
     */
    public void validate(EventFlowProperties properties) {
        EventFlowProperties.EventRetryConfig retry = properties.getRetry();
        if (retry.getProcessing().isDurableEnabled() && !retry.getDelivery().isEnabled()) {
            throw new IllegalStateException(
                    "event-flow.retry.processing.durable-enabled=true requires "
                            + "event-flow.retry.delivery.enabled=true: processing retries depend "
                            + "on the delivery boundary");
        }
    }
}
