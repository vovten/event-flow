package io.github.vovten.eventflow.autoconfig.transport.outgoing;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.transport.OutTransportFactory;
import io.github.vovten.eventflow.serialization.EventSerializationException;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.outgoing.EventKeyResolver;
import io.github.vovten.eventflow.transport.outgoing.EventKeyResolverBuilder;
import io.github.vovten.eventflow.transport.outgoing.KafkaOutTransport;
import org.springframework.beans.factory.BeanFactory;

/**
 * Factory for creating Kafka-based publisher event transports.
 * <p>
 * Supports configurable serialization format:
 * - "json" (default): UTF-8 encoded JSON, readable in Kafka UI tools
 * - "msgpack": Compact binary format for better performance
 * <p>
 * Supports a configurable Kafka record key strategy via {@code key}, parsed by the
 * framework-neutral {@link EventKeyResolverBuilder}:
 * - "none" (default), "process-id", "event-type", "metadata:&lt;name&gt;",
 *   or "bean:&lt;beanName&gt;" to resolve a user-defined {@link EventKeyResolver} bean.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class KafkaOutTransportFactory implements OutTransportFactory {

    private final EventSerializerFactory serializerFactory;
    private final BeanFactory beanFactory;

    /**
     * Creates the factory.
     *
     * @param serializerFactory factory for event serializers
     * @param beanFactory       Spring bean factory backing the {@code bean:} key strategy
     */
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
     * Resolve the {@link EventKeyResolver} strategy from the {@code key} configuration
     * using {@link EventKeyResolverBuilder}, wiring the {@code bean:} strategy to the
     * Spring {@link BeanFactory}.
     *
     * @param key the configured key strategy (may be null or blank)
     * @return the resolved key resolver
     * @throws IllegalArgumentException if the strategy is unknown or malformed
     */
    private EventKeyResolver createKeyResolver(String key) {
        return new EventKeyResolverBuilder(key)
                .withBeanLookup(beanName -> beanFactory.getBean(beanName, EventKeyResolver.class))
                .build();
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
        // Resolving through the builder doubles as validation: an unknown or malformed
        // 'key' (including a missing bean reference) fails at startup, not on first send.
        createKeyResolver(config.getKey());
    }
}
