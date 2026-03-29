package com.github.vovten.eventflow.autoconfig.transport.outgoing;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.OutTransportFactory;
import com.github.vovten.eventflow.transport.OutTransport;
import com.github.vovten.eventflow.transport.outgoing.KafkaOutTransport;

/**
 * Factory for creating Kafka-based publisher event transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
public class KafkaOutTransportFactory implements OutTransportFactory {

    @Override
    public String getName() {
        return "kafka";
    }

    @Override
    public OutTransport createPublisher(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new KafkaOutTransport(
            config.getServers(),
            config.getTopic()
        );
    }

    @Override
    public void validate(EventFlowProperties.TransportConfig config) {
        if (config.getServers() == null) {
            throw new IllegalStateException(
                "Kafka transport requires 'servers' configuration (e.g., 'localhost:9092' or 'kafka1:9092,kafka2:9092')"
            );
        }
        if (config.getTopic() == null) {
            throw new IllegalStateException(
                "Kafka transport requires topic configuration"
            );
        }
    }
}
