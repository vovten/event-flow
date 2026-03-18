package com.github.vovten.eventflow.autoconfig.transport.dispatcher;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.DispatcherTransportFactory;
import com.github.vovten.eventflow.transport.DispatcherTransport;
import com.github.vovten.eventflow.transport.dispatcher.KafkaDispatcherTransport;

/**
 * Factory for creating Kafka-based dispatcher event transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
public class KafkaDispatcherTransportFactory implements DispatcherTransportFactory {

    @Override
    public String getType() {
        return "kafka";
    }

    @Override
    public DispatcherTransport createDispatcher(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new KafkaDispatcherTransport(
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
