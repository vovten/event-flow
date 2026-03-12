package com.github.vovten.eventflow.autoconfig.transport;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.IncomingEventTransport;
import com.github.vovten.eventflow.transport.incoming.KafkaIncomingEventTransport;
import org.springframework.stereotype.Component;

/**
 * Factory for creating Kafka-based incoming event transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
@Component
public class KafkaIncomingTransportFactory implements IncomingTransportFactory {

    @Override
    public String getType() {
        return "kafka";
    }

    @Override
    public IncomingEventTransport createIncoming(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new KafkaIncomingEventTransport(
            config.      getBootstrapServers(),
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
