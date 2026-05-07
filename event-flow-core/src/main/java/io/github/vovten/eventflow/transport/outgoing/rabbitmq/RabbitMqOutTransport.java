package io.github.vovten.eventflow.transport.outgoing.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * RabbitMQ transport for sending events asynchronously.
 * <p>
 * Supports direct, fanout, and topic exchange types. Uses RabbitMQ's async API
 * with publisher confirms for reliable delivery.
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 */
public class RabbitMqOutTransport implements OutTransport, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqOutTransport.class);

    private final Connection connection;
    private Channel channel;
    private final EventSerializer serializer;
    private final String exchange;
    private final String exchangeType;
    private final String routingKey;
    private volatile boolean closed = false;

    /**
     * Create RabbitMQ transport with mock connection and channel (for testing).
     */
    RabbitMqOutTransport(Connection connection, String exchange, String routingKey) {
        this.serializer = new JsonEventSerializer();
        this.exchange = exchange;
        this.exchangeType = "direct";
        this.routingKey = routingKey;
        this.connection = connection;
        try {
            this.channel = connection.createChannel();
            channel.confirmSelect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create RabbitMQ transport with bootstrap URI.
     *
     * @param uri        RabbitMQ connection URI (e.g., "amqp://localhost:5672")
     * @param exchange   exchange name
     * @param routingKey routing key for messages
     */
    public RabbitMqOutTransport(String uri, String exchange, String routingKey) {
        this(uri, exchange, "direct", routingKey, new JsonEventSerializer());
    }

    /**
     * Create RabbitMQ transport with bootstrap URI and custom serializer.
     *
     * @param uri        RabbitMQ connection URI
     * @param exchange   exchange name
     * @param routingKey routing key for messages
     * @param serializer the event serializer
     */
    public RabbitMqOutTransport(String uri, String exchange, String routingKey, EventSerializer serializer) {
        this(uri, exchange, "direct", routingKey, serializer);
    }

    /**
     * Create RabbitMQ transport with exchange type.
     *
     * @param uri           RabbitMQ connection URI
     * @param exchange      exchange name
     * @param exchangeType  exchange type: "direct", "fanout", "topic"
     * @param routingKey    routing key for messages
     * @param serializer   the event serializer
     */
    public RabbitMqOutTransport(String uri, String exchange, String exchangeType,
                                 String routingKey, EventSerializer serializer) {
        this.serializer = Objects.requireNonNull(serializer, "Serializer must not be null");
        this.exchange = Objects.requireNonNull(exchange, "Exchange must not be null");
        this.exchangeType = Objects.requireNonNull(exchangeType, "Exchange type must not be null");
        this.routingKey = Objects.requireNonNull(routingKey, "Routing key must not be null");

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(Objects.requireNonNull(uri, "URI must not be null"));

            this.connection = factory.newConnection();
            this.channel = connection.createChannel();
            // Enable publisher confirms
            channel.confirmSelect();
            // Declare exchange
            channel.exchangeDeclare(exchange, parseExchangeType(exchangeType), true);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create RabbitMQ connection", e);
        }
    }

    private static BuiltinExchangeType parseExchangeType(String type) {
        return switch (type.toLowerCase()) {
            case "fanout" -> BuiltinExchangeType.FANOUT;
            case "topic" -> BuiltinExchangeType.TOPIC;
            default -> BuiltinExchangeType.DIRECT;
        };
    }

    @Override
    public String name() {
        return "rabbitmq";
    }

    @Override
    public CompletableFuture<SendResult> send(Event event) {
        if (closed) {
            throw new IllegalStateException("RabbitMqOutTransport is already closed");
        }
        CompletableFuture<SendResult> future = new CompletableFuture<>();
        try {
            byte[] body = serializer.serialize(event);
            String key = event.type().getName();

            channel.basicPublish(exchange, routingKey,
                    com.rabbitmq.client.MessageProperties.PERSISTENT_TEXT_PLAIN,
                    body);

            // Wait for confirm (synchronous for simplicity, async via callback possible)
            channel.waitForConfirmsOrDie(5000);
            future.complete(SendResult.success(exchange + ":" + routingKey, Map.of("exchange", exchange, "routingKey", routingKey)));
        } catch (Exception e) {
            future.complete(SendResult.failure(exchange + ":" + routingKey, e, Map.of()));
        }
        return future;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                if (channel != null && channel.isOpen()) {
                    channel.close();
                }
                if (connection != null && connection.isOpen()) {
                    connection.close();
                }
            } catch (Exception e) {
                log.warn("Error closing RabbitMQ transport", e);
            }
        }
    }
}
