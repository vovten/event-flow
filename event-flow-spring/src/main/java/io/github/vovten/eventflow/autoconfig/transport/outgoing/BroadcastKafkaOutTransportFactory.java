package io.github.vovten.eventflow.autoconfig.transport.outgoing;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.transport.OutTransportFactory;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.outgoing.BroadcastKafkaOutTransport;
import io.github.vovten.eventflow.transport.outgoing.EventKeyResolver;
import io.github.vovten.eventflow.transport.outgoing.EventKeyResolverBuilder;
import org.springframework.beans.factory.BeanFactory;

/**
 * Factory for creating broadcast Kafka-based publisher event transports.
 * <p>
 * This factory creates {@link BroadcastKafkaOutTransport} instances
 * that send events to all partitions of a Kafka topic.
 * <p>
 * Supports a configurable Kafka record key strategy via {@code key}, parsed by the
 * framework-neutral {@link EventKeyResolverBuilder}: "none" (default), "process-id",
 * "event-type", "metadata:&lt;name&gt;", or "bean:&lt;beanName&gt;". For broadcast transports
 * the key does not influence partition routing (records target every partition
 * explicitly); it is only written into the record metadata.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 * @see BroadcastKafkaOutTransport
 */
public class BroadcastKafkaOutTransportFactory implements OutTransportFactory {

    private final EventSerializerFactory serializerFactory;
    private final BeanFactory beanFactory;

    /**
     * Creates the factory.
     *
     * @param serializerFactory factory for event serializers
     * @param beanFactory       Spring bean factory backing the {@code bean:} key strategy
     */
    public BroadcastKafkaOutTransportFactory(EventSerializerFactory serializerFactory, BeanFactory beanFactory) {
        this.serializerFactory = serializerFactory;
        this.beanFactory = beanFactory;
    }

    /**
     * Creates the factory without a {@link BeanFactory}.
     *
     * @param serializerFactory factory for event serializers
     * @deprecated use {@link #BroadcastKafkaOutTransportFactory(EventSerializerFactory, BeanFactory)}
     *             (since 1.3.1); the {@code bean:} key strategy is unavailable without a bean factory
     */
    @Deprecated
    public BroadcastKafkaOutTransportFactory(EventSerializerFactory serializerFactory) {
        this(serializerFactory, null);
    }

    @Override
    public String getName() {
        return "broadcast-kafka";
    }

    @Override
    public OutTransport createPublisher(EventFlowProperties.TransportConfig config) {
        validate(config);
        EventSerializer serializer = createSerializer(config.getSerialization());
        EventKeyResolver keyResolver = createKeyResolver(config.getKey());
        return new BroadcastKafkaOutTransport(
            config.getServers(),
            config.getTopics(),
            serializer,
            keyResolver
        );
    }

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
        EventKeyResolverBuilder builder = new EventKeyResolverBuilder(key);
        if (beanFactory != null) {
            builder.withBeanLookup(beanName -> beanFactory.getBean(beanName, EventKeyResolver.class));
        }
        // without a bean factory the builder rejects a 'bean:' spec with an
        // IllegalArgumentException instead of an NPE deep in the lookup lambda
        return builder.build();
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
        // Resolving through the builder doubles as validation: an unknown or malformed
        // 'key' (including a missing bean reference) fails at startup, not on first send.
        createKeyResolver(config.getKey());
    }
}
