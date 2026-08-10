package io.github.vovten.eventflow.transport.rabbitmq;

import com.rabbitmq.client.Connection;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.transport.rabbitmq.incoming.RabbitMqInTransport;
import io.github.vovten.eventflow.transport.rabbitmq.outgoing.RabbitMqOutTransport;

import java.util.Objects;

/**
 * Builder for creating a pair of RabbitMQ dispatcher and publisher event transports.
 * <p>
 * This builder creates both transports sharing the same broker connection via a
 * {@link RabbitMqConnectionHolder}. The dispatcher transport consumes from the
 * configured queue and the publisher transport publishes to the configured
 * exchange, so a published event routed to the queue is delivered to the
 * dispatcher consumer through one shared connection.
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * RabbitMqTransportsBuilder builder = new RabbitMqTransportsBuilder()
 *     .host("localhost")
 *     .queue("events");
 *
 * RabbitMqTransportsBuilder.RabbitMqTransports transports = builder.build();
 * RabbitMqOutTransport publisher = transports.publisher();
 * RabbitMqInTransport dispatcher = transports.dispatcher();
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public class RabbitMqTransportsBuilder {

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 5672;
    private static final String DEFAULT_VIRTUAL_HOST = "/";
    private static final String DEFAULT_USERNAME = "guest";
    private static final String DEFAULT_PASSWORD = "guest";

    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    private String virtualHost = DEFAULT_VIRTUAL_HOST;
    private String username = DEFAULT_USERNAME;
    private String password = DEFAULT_PASSWORD;
    private String queue = "event-flow";
    private String exchange = "";
    private String routingKey;
    private boolean queueDurable = false;
    private EventSerializerFactory serializerFactory = new EventSerializerFactory();
    private EventSerializer serializer = new JsonEventSerializer();
    private RabbitMqConnectionFactory connectionFactory = new RabbitMqConnectionFactory();

    /**
     * Set the RabbitMQ broker host.
     *
     * @param host the broker host
     * @return this builder
     */
    public RabbitMqTransportsBuilder host(String host) {
        this.host = host;
        return this;
    }

    /**
     * Set the RabbitMQ broker port.
     *
     * @param port the broker port
     * @return this builder
     */
    public RabbitMqTransportsBuilder port(int port) {
        this.port = port;
        return this;
    }

    /**
     * Set the RabbitMQ virtual host.
     *
     * @param virtualHost the virtual host
     * @return this builder
     */
    public RabbitMqTransportsBuilder virtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
        return this;
    }

    /**
     * Set the RabbitMQ user name.
     *
     * @param username the user name
     * @return this builder
     */
    public RabbitMqTransportsBuilder username(String username) {
        this.username = username;
        return this;
    }

    /**
     * Set the RabbitMQ password.
     *
     * @param password the password
     * @return this builder
     */
    public RabbitMqTransportsBuilder password(String password) {
        this.password = password;
        return this;
    }

    /**
     * Set the queue name consumed by the dispatcher transport.
     * <p>
     * When no routing key is configured, the routing key defaults to this queue
     * name so that publishing on the default exchange reaches the queue.
     *
     * @param queue the queue name
     * @return this builder
     */
    public RabbitMqTransportsBuilder queue(String queue) {
        this.queue = queue;
        return this;
    }

    /**
     * Set the exchange name used by the publisher transport.
     * <p>
     * An empty string (the default) means the RabbitMQ default direct exchange.
     *
     * @param exchange the exchange name
     * @return this builder
     */
    public RabbitMqTransportsBuilder exchange(String exchange) {
        this.exchange = exchange;
        return this;
    }

    /**
     * Set the routing key used by the publisher transport.
     * <p>
     * When not set, the routing key defaults to the configured queue name.
     *
     * @param routingKey the routing key
     * @return this builder
     */
    public RabbitMqTransportsBuilder routingKey(String routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    /**
     * Set whether the dispatcher queue is declared durable.
     *
     * @param queueDurable true to declare the queue durable
     * @return this builder
     */
    public RabbitMqTransportsBuilder queueDurable(boolean queueDurable) {
        this.queueDurable = queueDurable;
        return this;
    }

    /**
     * Set the serializer factory used by the dispatcher transport for
     * magic-byte detection during deserialization.
     *
     * @param serializerFactory the serializer factory
     * @return this builder
     */
    public RabbitMqTransportsBuilder serializerFactory(EventSerializerFactory serializerFactory) {
        this.serializerFactory = Objects.requireNonNull(serializerFactory, "serializerFactory must not be null");
        return this;
    }

    /**
     * Set the serializer used by the publisher transport.
     *
     * @param serializer the event serializer
     * @return this builder
     */
    public RabbitMqTransportsBuilder serializer(EventSerializer serializer) {
        this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
        return this;
    }

    /**
     * Set the connection factory used to open the shared broker connection.
     *
     * @param connectionFactory the connection factory
     * @return this builder
     */
    public RabbitMqTransportsBuilder connectionFactory(RabbitMqConnectionFactory connectionFactory) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory must not be null");
        return this;
    }

    /**
     * Build and return a pair of RabbitMQ transports sharing one connection.
     *
     * @return a pair of dispatcher and publisher transports
     */
    public RabbitMqTransports build() {
        Connection connection = configuredConnectionFactory().newConnection();
        RabbitMqConnectionHolder holder = new RabbitMqConnectionHolder(connection);
        RabbitMqInTransport dispatcher = new RabbitMqInTransport(connection, queue, queueDurable,
                serializerFactory, holder);
        RabbitMqOutTransport publisher = new RabbitMqOutTransport(connection, exchange,
                resolvedRoutingKey(), serializer, holder);
        return new RabbitMqTransports(dispatcher, publisher);
    }

    private RabbitMqConnectionFactory configuredConnectionFactory() {
        return connectionFactory
                .host(host)
                .port(port)
                .virtualHost(virtualHost)
                .username(username)
                .password(password);
    }

    private String resolvedRoutingKey() {
        return routingKey != null ? routingKey : queue;
    }

    /**
     * Record holding a pair of RabbitMQ transports.
     *
     * @param dispatcher the dispatcher transport
     * @param publisher  the publisher transport
     */
    public record RabbitMqTransports(
            RabbitMqInTransport dispatcher,
            RabbitMqOutTransport publisher
    ) {
    }
}
