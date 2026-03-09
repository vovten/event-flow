package com.github.vovten.eventflow.autoconfig.transport;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.IncomingEventTransport;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;

/**
 * Factory interface for creating transports based on configuration.
 * <p>
 * Implementations should be annotated with {@code @Component} to be
 * automatically discovered by {@link EventFlowAutoConfiguration}.
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * @Component
 * public class KafkaTransportFactory implements TransportFactory {
 *     
 *     @Override
 *     public String getType() {
 *         return "kafka";
 *     }
 *     
 *     @Override
 *     public OutgoingEventTransport createOutgoing(ChannelConfig config) {
 *         return new KafkaOutgoingEventTransport(config.getBootstrapServers(), config.getTopic());
 *     }
 *     
 *     @Override
 *     public IncomingEventTransport createIncoming(TransportConfig config) {
 *         return new KafkaIncomingEventTransport(config.getBootstrapServers(), config.getTopic(), config.getConsumerGroup());
 *     }
 * }
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
public interface TransportFactory {
    
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
     * Create incoming transport from transport configuration.
     *
     * @param config transport configuration
     * @return incoming transport instance
     */
    IncomingEventTransport createIncoming(EventFlowProperties.TransportConfig config);
    
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
