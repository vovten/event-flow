package io.github.vovten.eventflow.autoconfig.transport;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.config.DispatcherConfiguration;
import io.github.vovten.eventflow.transport.InTransport;

/**
 * Factory for creating dispatcher event transports.
 * <p>
 * Implementations should be annotated with {@code @Component} to be
 * automatically discovered by {@link DispatcherConfiguration}.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public interface InTransportFactory {

    /**
     * @return the type identifier this factory handles (e.g., "kafka", "in-memory")
     */
    String getType();

    /**
     * Create dispatcher transport from transport configuration.
     *
     * @param config transport configuration
     * @return dispatcher transport instance
     */
    InTransport createDispatcher(EventFlowProperties.TransportConfig config);

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
