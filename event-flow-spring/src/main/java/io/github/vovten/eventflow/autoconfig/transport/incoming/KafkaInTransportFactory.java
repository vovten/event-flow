package io.github.vovten.eventflow.autoconfig.transport.incoming;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.transport.InTransportFactory;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.InTransport;
import io.github.vovten.eventflow.transport.incoming.KafkaInTransport;

/**
 * Factory for creating Kafka-based dispatcher event transports.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-10
 */
public class KafkaInTransportFactory implements InTransportFactory {

    private final EventSerializerFactory serializerFactory;

    public KafkaInTransportFactory(EventSerializerFactory serializerFactory) {
        this.serializerFactory = serializerFactory;
    }

    @Override
    public String getType() {
        return "kafka";
    }

    @Override
    public InTransport createDispatcher(EventFlowProperties.TransportConfig config) {
        validate(config);
        return new KafkaInTransport(
            config.getServers(),
            config.getTopics(),
            config.getConsumerGroup()
        );
    }

    /**
     * Create Kafka transport with custom serializer factory.
     *
     * @param config transport configuration
     * @param serializerFactory serializer factory to use
     * @return Kafka transport
     */
    public InTransport createDispatcher(EventFlowProperties.TransportConfig config, EventSerializerFactory serializerFactory) {
        validate(config);
        return new KafkaInTransport(
            config.getServers(),
            config.getTopics(),
            config.getConsumerGroup()
        );
    }

    @Override
    public void validate(EventFlowProperties.TransportConfig config) {
        if (config.getServers() == null) {
            throw new IllegalStateException(
                    "Kafka transport requires 'servers' configuration (e.g., 'localhost:9092' or 'kafka1:9092,kafka2:9092')"
            );
        }
        if (config.getTopics() == null) {
            throw new IllegalStateException(
                    "Kafka transport requires topics configuration"
            );
        }
    }
}
