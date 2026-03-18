package com.github.vovten.eventflow.autoconfig.transport.publisher;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.PublisherTransportFactory;
import com.github.vovten.eventflow.transport.PublisherTransport;
import com.github.vovten.eventflow.transport.publisher.BroadcastKafkaPublisherTransport;

/**
 * Factory for creating broadcast Kafka-based publisher event transports.
 * <p>
 * This factory creates {@link BroadcastKafkaPublisherTransport} instances
 * that send events to all partitions of a Kafka topic.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-13
 * @see BroadcastKafkaPublisherTransport
 */
public class BroadcastKafkaPublisherTransportFactory implements PublisherTransportFactory {

    @Override
    public String getName() {
        return "broadcast-kafka";
    }

    @Override
    public PublisherTransport createPublisher(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new BroadcastKafkaPublisherTransport(
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
