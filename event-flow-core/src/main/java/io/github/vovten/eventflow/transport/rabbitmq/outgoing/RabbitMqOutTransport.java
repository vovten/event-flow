package io.github.vovten.eventflow.transport.rabbitmq.outgoing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.json.JsonEventSerializer;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.TransportException;
import io.github.vovten.eventflow.transport.rabbitmq.RabbitMqConnectionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RabbitMQ outgoing transport for sending events to an exchange.
 * <p>
 * {@link #send(Event)} serializes the event with the configured
 * {@link EventSerializer} (default {@link JsonEventSerializer}) and publishes
 * the body to the configured exchange with a routing key. An empty exchange
 * means the RabbitMQ default direct exchange, so the routing key maps directly
 * to a queue name. Publishing on a closed transport throws
 * {@link IllegalStateException}.
 * <p>
 * The transport shares one broker connection with its incoming counterpart via
 * a {@link RabbitMqConnectionHolder}. The holder reference is acquired when the
 * channel is opened and released on {@link #close()}.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public class RabbitMqOutTransport implements OutTransport, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqOutTransport.class);

    private final Channel channel;
    private final String exchange;
    private final String routingKey;
    private final EventSerializer serializer;
    private final RabbitMqConnectionHolder connectionHolder;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a RabbitMQ outgoing transport publishing to the supplied exchange.
     *
     * @param connection       the shared broker connection
     * @param exchange         the exchange name (empty string means the default exchange)
     * @param routingKey       the routing key for publishing
     * @param serializer       the event serializer to use
     * @param connectionHolder the shared connection owner
     */
    public RabbitMqOutTransport(Connection connection, String exchange, String routingKey,
                                EventSerializer serializer, RabbitMqConnectionHolder connectionHolder) {
        this.exchange = Objects.requireNonNull(exchange, "exchange must not be null");
        this.routingKey = Objects.requireNonNull(routingKey, "routingKey must not be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
        this.connectionHolder = Objects.requireNonNull(connectionHolder, "connectionHolder must not be null");
        connectionHolder.acquire();
        try {
            this.channel = openChannel(connection);
        } catch (RuntimeException e) {
            connectionHolder.release();
            throw e;
        }
    }

    private static Channel openChannel(Connection connection) {
        try {
            return connection.createChannel();
        } catch (Exception e) {
            throw new TransportException("Failed to open RabbitMQ channel", e);
        }
    }

    @Override
    public String name() {
        return "rabbitmq";
    }

    @Override
    public CompletableFuture<SendResult> send(Event event) {
        if (closed.get()) {
            throw new IllegalStateException("RabbitMqOutTransport is already closed");
        }
        try {
            byte[] body = serializer.serialize(event);
            channel.basicPublish(exchange, routingKey, null, body);
            return CompletableFuture.completedFuture(SendResult.success(destination()));
        } catch (Exception e) {
            log.error("Failed to publish event to exchange '{}' with routing key '{}'",
                    exchange, routingKey, e);
            return CompletableFuture.completedFuture(SendResult.failure(destination(), e, e.getMessage()));
        }
    }

    /**
     * Close the channel and release the shared connection reference.
     * <p>
     * Safe to call multiple times and from any thread.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            closeChannel();
            connectionHolder.release();
            log.debug("RabbitMqOutTransport closed for exchange '{}'", exchange);
        }
    }

    private void closeChannel() {
        try {
            channel.close();
        } catch (Exception e) {
            log.warn("Error closing RabbitMQ channel for exchange '{}'", exchange, e);
        }
    }

    private String destination() {
        return name() + "-" + (exchange.isEmpty() ? "default" : exchange) + "/" + routingKey;
    }
}
