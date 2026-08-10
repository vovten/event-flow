package io.github.vovten.eventflow.autoconfig.transport.incoming;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.transport.InTransportFactory;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.InTransport;
import io.github.vovten.eventflow.transport.rabbitmq.RabbitMqConnectionFactory;
import io.github.vovten.eventflow.transport.rabbitmq.RabbitMqTransportsBuilder;

import java.util.Objects;

/**
 * Factory for creating RabbitMQ dispatcher event transports.
 * <p>
 * Maps the {@link EventFlowProperties.TransportConfig} connection and queue
 * settings to the core {@link RabbitMqTransportsBuilder} and returns the
 * dispatcher transport of the built pair. The unused publisher transport is
 * closed immediately so the shared connection is released when the dispatcher
 * stops.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public class RabbitMqInTransportFactory implements InTransportFactory {

    private final EventSerializerFactory serializerFactory;
    private final RabbitMqConnectionFactory connectionFactory;

    /**
     * Create a factory backed by a new {@link RabbitMqConnectionFactory}.
     *
     * @param serializerFactory the serializer factory for magic-byte detection
     */
    public RabbitMqInTransportFactory(EventSerializerFactory serializerFactory) {
        this(serializerFactory, new RabbitMqConnectionFactory());
    }

    /**
     * Create a factory backed by the supplied {@link RabbitMqConnectionFactory}.
     *
     * @param serializerFactory the serializer factory for magic-byte detection
     * @param connectionFactory the connection factory used to open the broker connection
     */
    public RabbitMqInTransportFactory(EventSerializerFactory serializerFactory,
                                      RabbitMqConnectionFactory connectionFactory) {
        this.serializerFactory = Objects.requireNonNull(serializerFactory, "serializerFactory must not be null");
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory must not be null");
    }

    @Override
    public String getType() {
        return "rabbitmq";
    }

    @Override
    public InTransport createDispatcher(EventFlowProperties.TransportConfig config) {
        validate(config);
        RabbitMqTransportsBuilder.RabbitMqTransports transports = createBuilder(config).build();
        transports.publisher().close();
        return transports.dispatcher();
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
                .serializerFactory(serializerFactory);
    }
}
