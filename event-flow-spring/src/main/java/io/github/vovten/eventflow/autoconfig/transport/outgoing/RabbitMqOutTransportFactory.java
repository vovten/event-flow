package io.github.vovten.eventflow.autoconfig.transport.outgoing;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.transport.OutTransportFactory;
import io.github.vovten.eventflow.serialization.EventSerializationException;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.rabbitmq.RabbitMqConnectionFactory;
import io.github.vovten.eventflow.transport.rabbitmq.RabbitMqTransportsBuilder;

import java.util.Objects;

/**
 * Factory for creating RabbitMQ publisher event transports.
 * <p>
 * Maps the {@link EventFlowProperties.TransportConfig} connection, exchange and
 * routing key settings to the core {@link RabbitMqTransportsBuilder} and returns
 * the publisher transport of the built pair. The unused dispatcher transport of
 * the pair is never started, so it holds no connection reference.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public class RabbitMqOutTransportFactory implements OutTransportFactory {

    private final EventSerializerFactory serializerFactory;
    private final RabbitMqConnectionFactory connectionFactory;

    /**
     * Create a factory backed by a new {@link RabbitMqConnectionFactory}.
     *
     * @param serializerFactory the serializer factory for creating event serializers
     */
    public RabbitMqOutTransportFactory(EventSerializerFactory serializerFactory) {
        this(serializerFactory, new RabbitMqConnectionFactory());
    }

    /**
     * Create a factory backed by the supplied {@link RabbitMqConnectionFactory}.
     *
     * @param serializerFactory the serializer factory for creating event serializers
     * @param connectionFactory the connection factory used to open the broker connection
     */
    public RabbitMqOutTransportFactory(EventSerializerFactory serializerFactory,
                                       RabbitMqConnectionFactory connectionFactory) {
        this.serializerFactory = Objects.requireNonNull(serializerFactory, "serializerFactory must not be null");
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory must not be null");
    }

    @Override
    public String getName() {
        return "rabbitmq";
    }

    @Override
    public OutTransport createPublisher(EventFlowProperties.TransportConfig config) {
        validate(config);
        RabbitMqTransportsBuilder.RabbitMqTransports transports = createBuilder(config).build();
        transports.dispatcher().close();
        return transports.publisher();
    }

    /**
     * Create the event serializer based on the configured serialization format.
     * <p>
     * Uses {@link EventSerializerFactory#getByName(String)} to look up the
     * serializer, supporting both built-in formats (json, msgpack) and custom
     * serializers registered in the factory.
     *
     * @param config the transport configuration
     * @return the event serializer for the configured format
     * @throws EventSerializationException if the format is unknown
     */
    private EventSerializer createSerializer(EventFlowProperties.TransportConfig config) {
        String format = config.getSerialization();
        String name = (format == null || format.isBlank()) ? "json" : format;
        return serializerFactory.getByName(name);
    }

    @Override
    public void validate(EventFlowProperties.TransportConfig config) {
        if (config.getQueue() == null || config.getQueue().isBlank()) {
            throw new IllegalStateException("RabbitMQ transport requires 'queue' configuration");
        }
    }

    private RabbitMqTransportsBuilder createBuilder(EventFlowProperties.TransportConfig config) {
        return new RabbitMqTransportsBuilder()
                .connectionFactory(connectionFactory)
                .host(config.getHost())
                .port(config.getPort())
                .virtualHost(config.getVirtualHost())
                .username(config.getUsername())
                .password(config.getPassword())
                .queue(config.getQueue())
                .exchange(config.getExchange())
                .routingKey(config.getRoutingKey())
                .queueDurable(config.isQueueDurable())
                .serializer(createSerializer(config));
    }
}
