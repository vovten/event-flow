package io.github.vovten.eventflow.autoconfig.transport.outgoing;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.transport.OutTransportFactory;
import io.github.vovten.eventflow.serialization.EventSerializationException;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.outgoing.EventKeyResolver;
import io.github.vovten.eventflow.transport.outgoing.KafkaOutTransport;
import org.springframework.beans.factory.BeanFactory;

/**
 * Factory for creating Kafka-based publisher event transports.
 * <p>
 * Supports configurable serialization format:
 * - "json" (default): UTF-8 encoded JSON, readable in Kafka UI tools
 * - "msgpack": Compact binary format for better performance
 * <p>
 * Supports a configurable Kafka record key strategy via {@code key}:
 * - "none" (default), "process-id", "event-type", "metadata:&lt;name&gt;",
 *   or "bean:&lt;beanName&gt;" to resolve a user-defined {@link EventKeyResolver} bean.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class KafkaOutTransportFactory implements OutTransportFactory {

    private static final String PREFIX_METADATA = "metadata:";
    private static final String PREFIX_BEAN = "bean:";

    private final EventSerializerFactory serializerFactory;
    private final BeanFactory beanFactory;

    public KafkaOutTransportFactory(EventSerializerFactory serializerFactory, BeanFactory beanFactory) {
        this.serializerFactory = serializerFactory;
        this.beanFactory = beanFactory;
    }

    @Override
    public String getName() {
        return "kafka";
    }

    @Override
    public OutTransport createPublisher(EventFlowProperties.TransportConfig config) {
        validate(config);
        EventSerializer serializer = createSerializer(config.getSerialization());
        EventKeyResolver keyResolver = createKeyResolver(config.getKey());
        return new KafkaOutTransport(
            config.getServers(),
            config.getTopics(),
            serializer,
            keyResolver
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
        return serializerFactory.getByName(name);
    }

    /**
     * Resolve the {@link EventKeyResolver} strategy from the {@code key} configuration.
     *
     * @param key the configured key strategy (may be null or blank)
     * @return the resolved key resolver
     * @throws IllegalArgumentException if the strategy is unknown
     */
    private EventKeyResolver createKeyResolver(String key) {
        if (key == null || key.isBlank() || "none".equalsIgnoreCase(key)) {
            return EventKeyResolver.none();
        }
        if ("process-id".equalsIgnoreCase(key)) {
            return EventKeyResolver.processId();
        }
        if ("event-type".equalsIgnoreCase(key)) {
            return EventKeyResolver.eventType();
        }
        if (key.regionMatches(true, 0, PREFIX_METADATA, 0, PREFIX_METADATA.length())) {
            String metadataKey = key.substring(PREFIX_METADATA.length());
            if (metadataKey.isBlank()) {
                throw new IllegalArgumentException(
                        "Kafka 'key' strategy 'metadata:' requires a metadata name, e.g. 'metadata:orderId'"
                );
            }
            return EventKeyResolver.metadata(metadataKey);
        }
        if (key.regionMatches(true, 0, PREFIX_BEAN, 0, PREFIX_BEAN.length())) {
            String beanName = key.substring(PREFIX_BEAN.length());
            if (beanName.isBlank()) {
                throw new IllegalArgumentException(
                        "Kafka 'key' strategy 'bean:' requires a bean name, e.g. 'bean:orderKeyResolver'"
                );
            }
            return beanFactory.getBean(beanName, EventKeyResolver.class);
        }
        throw new IllegalArgumentException(
                "Unknown Kafka 'key' strategy '" + key + "'. Supported values: "
                        + "none, process-id, event-type, metadata:<name>, bean:<beanName>"
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