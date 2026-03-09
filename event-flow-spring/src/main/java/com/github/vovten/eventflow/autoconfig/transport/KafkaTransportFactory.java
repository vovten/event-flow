package com.github.vovten.eventflow.autoconfig.transport;

import com.github.vovten.eventflow.autoconfig.EventFlowAutoConfiguration;
import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.IncomingEventTransport;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import com.github.vovten.eventflow.transport.incoming.KafkaIncomingEventTransport;
import com.github.vovten.eventflow.transport.outgoing.KafkaOutgoingEventTransport;
import org.springframework.stereotype.Component;

/**
 * Transport factory for Kafka-based event transports.
 * <p>
 * Automatically discovered by {@link EventFlowAutoConfiguration} when
 * Kafka classes are present on the classpath.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@Component
public class KafkaTransportFactory implements TransportFactory {
    
    @Override
    public String getType() {
        return "kafka";
    }
    
    @Override
    public OutgoingEventTransport createOutgoing(EventFlowProperties.ChannelConfig config) {
        validate(config);
        return new KafkaOutgoingEventTransport(
            config.getBootstrapServers(),
            config.getTopic()
        );
    }
    
    @Override
    public IncomingEventTransport createIncoming(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new KafkaIncomingEventTransport(
            config.getBootstrapServers(),
            config.getTopic(),
            config.getConsumerGroup()
        );
    }
    
    @Override
    public void validate(EventFlowProperties.ChannelConfig config) {
        if (config.getBootstrapServers() == null) {
            throw new IllegalStateException(
                "Kafka channel requires bootstrap-servers configuration"
            );
        }
        if (config.getTopic() == null) {
            throw new IllegalStateException(
                "Kafka channel requires topic configuration"
            );
        }
    }
    
    @Override
    public void validate(EventFlowProperties.TransportConfig config) {
        if (config.getBootstrapServers() == null) {
            throw new IllegalStateException(
                "Kafka transport requires bootstrap-servers configuration"
            );
        }
        if (config.getTopic() == null) {
            throw new IllegalStateException(
                "Kafka transport requires topic configuration"
            );
        }
    }
}
