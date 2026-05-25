package io.github.vovten.eventflow.autoconfig.transport.outgoing;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.transport.OutTransportFactory;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.outgoing.BroadcastKafkaOutTransport;

/**
 * Factory for creating broadcast Kafka-based publisher event transports.
 * <p>
 * This factory creates {@link BroadcastKafkaOutTransport} instances
 * that send events to all partitions of a Kafka topic.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 * @see BroadcastKafkaOutTransport
 */
public class BroadcastKafkaOutTransportFactory implements OutTransportFactory {

    private final EventSerializerFactory serializerFactory;

    public BroadcastKafkaOutTransportFactory(EventSerializerFactory serializerFactory) {
        this.serializerFactory = serializerFactory;
    }

    @Override
    public String getName() {
        return "broadcast-kafka";
    }

    @Override
    public OutTransport createPublisher(EventFlowProperties.TransportConfig config) {
        validate(config);
        EventSerializer serializer = createSerializer(config.getSerialization());
        return new BroadcastKafkaOutTransport(
            config.getServers(),
            config.getTopics(),
            serializer
        );
    }

    private EventSerializer createSerializer(String format) {
        String name = (format == null || format.isBlank()) ? "json" : format;
        return serializerFactory.getByName(name);
    }

    @Override
    public void validate(EventFlowProperties.TransportConfig config) {
        if (config.getServers() == null) {
            throw new IllegalStateException(
                    "Broadcast Kafka transport requires 'servers' configuration (e.g., 'localhost:9092' or 'kafka1:9092,kafka2:9092')"
            );
        }
        if (config.getTopics() == null) {
            throw new IllegalStateException(
                    "Broadcast Kafka transport requires topics configuration"
            );
        }
    }
}
