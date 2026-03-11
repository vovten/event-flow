package com.github.vovten.eventflow.autoconfig.transport;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.config.ChannelConfiguration;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;

/**
 * Factory for creating outgoing event transports.
 * <p>
 * Implementations should be annotated with {@code @Component} to be
 * automatically discovered by {@link ChannelConfiguration}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
public interface OutgoingTransportFactory {

    /**
     * @return the type identifier this factory handles (e.g., "kafka", "in-memory")
     */
    String getType();

    /**
     * Create outgoing transport from channel configuration.
     *
     * @param config channel configuration
     * @return outgoing transport instance
     */
    OutgoingEventTransport createOutgoing(EventFlowProperties.ChannelConfig config);

    /**
     * Validate channel configuration.
     * Override to add custom validation logic.
     *
     * @param config channel configuration to validate
     * @throws IllegalStateException if configuration is invalid
     */
    default void validate(EventFlowProperties.ChannelConfig config) {
        // Default no-op validation
    }
}
