package com.github.vovten.eventflow.autoconfig.transport;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import com.github.vovten.eventflow.transport.outgoing.BroadcastKafkaOutgoingEventTransport;
import org.springframework.stereotype.Component;

/**
 * Factory for creating broadcast Kafka-based outgoing event transports.
 * <p>
 * This factory creates {@link BroadcastKafkaOutgoingEventTransport} instances
 * that send events to all partitions of a Kafka topic.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-13
 * @see BroadcastKafkaOutgoingEventTransport
 */
@Component
public class BroadcastKafkaOutgoingTransportFactory implements OutgoingTransportFactory {

    @Override
    public String getName() {
        return "broadcast-kafka";
    }

    @Override
    public OutgoingEventTransport createOutgoing(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new BroadcastKafkaOutgoingEventTransport(
            config.getBootstrapServers(),
            config.getTopic()
        );
    }

    @Override
    public void validate(EventFlowProperties.TransportConfig config) {
        if (config.getBootstrapServers() == null) {
            throw new IllegalStateException(
                "Broadcast Kafka transport requires bootstrap-servers configuration"
            );
        }
        if (config.getTopic() == null) {
            throw new IllegalStateException(
                "Broadcast Kafka transport requires topic configuration"
            );
        }
    }
}
