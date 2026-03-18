package com.github.vovten.eventflow.autoconfig.transport.publisher;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.PublisherTransportFactory;
import com.github.vovten.eventflow.transport.PublisherTransport;
import com.github.vovten.eventflow.transport.publisher.KafkaPublisherTransport;

/**
 * Factory for creating Kafka-based publisher event transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
public class KafkaPublisherTransportFactory implements PublisherTransportFactory {

    @Override
    public String getName() {
        return "kafka";
    }

    @Override
    public PublisherTransport createPublisher(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new KafkaPublisherTransport(
            config.getBootstrapServers(),
            config.getTopic()
        );
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
