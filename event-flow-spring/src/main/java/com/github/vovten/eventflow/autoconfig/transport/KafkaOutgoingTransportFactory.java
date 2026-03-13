package com.github.vovten.eventflow.autoconfig.transport;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import com.github.vovten.eventflow.transport.outgoing.KafkaOutgoingEventTransport;
import org.springframework.stereotype.Component;

/**
 * Factory for creating Kafka-based outgoing event transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
@Component
public class KafkaOutgoingTransportFactory implements OutgoingTransportFactory {

    @Override
    public String getName() {
        return "kafka";
    }

    @Override
    public OutgoingEventTransport createOutgoing(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new KafkaOutgoingEventTransport(
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
