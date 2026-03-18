package com.github.vovten.eventflow.autoconfig.transport;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.config.ChannelConfiguration;
import com.github.vovten.eventflow.transport.PublisherTransport;

/**
 * Factory for creating publisher event transports.
 * <p>
 * Implementations should be annotated with {@code @Component} to be
 * automatically discovered by {@link ChannelConfiguration}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
public interface PublisherTransportFactory {

    /**
     * @return the name identifier this factory handles (e.g., "kafka", "in-memory")
     */
    String getName();

    /**
     * Create publisher transport from transport configuration.
     *
     * @param config transport configuration
     * @return publisher transport instance
     */
    PublisherTransport createPublisher(EventFlowProperties.TransportConfig config);

    /**
     * Validate transport configuration.
     * Override to add custom validation logic.
     *
     * @param config transport configuration to validate
     * @throws IllegalStateException if configuration is invalid
     */
    default void validate(EventFlowProperties.TransportConfig config) {
        // Default no-op validation
    }
}
