package com.github.vovten.eventflow.autoconfig.transport.incoming;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.InTransportFactory;
import com.github.vovten.eventflow.transport.InTransport;
import com.github.vovten.eventflow.transport.incoming.KafkaInTransport;

/**
 * Factory for creating Kafka-based dispatcher event transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
public class KafkaInTransportFactory implements InTransportFactory {

    @Override
    public String getType() {
        return "kafka";
    }

    @Override
    public InTransport createDispatcher(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new KafkaInTransport(
            config.getBootstrapServers(),
            config.getTopic(),
            config.getConsumerGroup()
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
