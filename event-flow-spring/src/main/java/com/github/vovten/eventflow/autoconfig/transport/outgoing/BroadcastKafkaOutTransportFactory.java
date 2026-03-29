package com.github.vovten.eventflow.autoconfig.transport.outgoing;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.OutTransportFactory;
import com.github.vovten.eventflow.transport.OutTransport;
import com.github.vovten.eventflow.transport.outgoing.BroadcastKafkaOutTransport;

/**
 * Factory for creating broadcast Kafka-based publisher event transports.
 * <p>
 * This factory creates {@link BroadcastKafkaOutTransport} instances
 * that send events to all partitions of a Kafka topic.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-13
 * @see BroadcastKafkaOutTransport
 */
public class BroadcastKafkaOutTransportFactory implements OutTransportFactory {

    @Override
    public String getName() {
        return "broadcast-kafka";
    }

    @Override
    public OutTransport createPublisher(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new BroadcastKafkaOutTransport(
            config.getServers(),
            config.getTopic()
        );
    }

    @Override
    public void validate(EventFlowProperties.TransportConfig config) {
        if (config.getServers() == null) {
            throw new IllegalStateException(
                "Broadcast Kafka transport requires 'servers' configuration (e.g., 'localhost:9092' or 'kafka1:9092,kafka2:9092')"
            );
        }
        if (config.getTopic() == null) {
            throw new IllegalStateException(
                "Broadcast Kafka transport requires topic configuration"
            );
        }
    }
}
