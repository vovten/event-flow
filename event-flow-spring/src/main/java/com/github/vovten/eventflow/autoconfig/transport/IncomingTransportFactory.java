package com.github.vovten.eventflow.autoconfig.transport;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.config.DispatcherConfiguration;
import com.github.vovten.eventflow.transport.IncomingEventTransport;

/**
 * Factory for creating incoming event transports.
 * <p>
 * Implementations should be annotated with {@code @Component} to be
 * automatically discovered by {@link DispatcherConfiguration}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
public interface IncomingTransportFactory {

    /**
     * @return the type identifier this factory handles (e.g., "kafka", "in-memory")
     */
    String getType();

    /**
     * Create incoming transport from transport configuration.
     *
     * @param config transport configuration
     * @return incoming transport instance
     */
    IncomingEventTransport createIncoming(EventFlowProperties.TransportConfig config);

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
