package com.github.vovten.eventflow.autoconfig.transport.outgoing;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.OutTransportFactory;
import com.github.vovten.eventflow.serialization.EventSerializationException;
import com.github.vovten.eventflow.serialization.EventSerializer;
import com.github.vovten.eventflow.serialization.EventSerializerFactory;
import com.github.vovten.eventflow.transport.OutTransport;
import com.github.vovten.eventflow.transport.outgoing.KafkaOutTransport;

/**
 * Factory for creating Kafka-based publisher event transports.
 * <p>
 * Supports configurable serialization format:
 * - "json" (default): UTF-8 encoded JSON, readable in Kafka UI tools
 * - "msgpack": Compact binary format for better performance
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
        EventSerializer serializer = createSerializer(config.getSerialization());
        return new KafkaOutTransport(
            config.getServers(),
            config.getTopic(),
            serializer
        );
    }

    /**
     * Create event serializer based on format configuration.
     * <p>
     * Uses {@link EventSerializerFactory#getByName(String)} to look up the serializer
     * by name, supporting both built-in formats (json, msgpack) and custom serializers.
     *
     * @param format serialization format name (e.g., "json", "msgpack", or custom)
     * @return appropriate EventSerializer instance
     * @throws EventSerializationException if format is unknown
     */
    private EventSerializer createSerializer(String format) {
        String name = (format == null || format.isBlank()) ? "json" : format;
        return EventSerializerFactory.getByName(name);
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
