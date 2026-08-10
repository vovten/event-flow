package io.github.vovten.eventflow.transport.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.vovten.eventflow.transport.TransportException;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * Factory for creating RabbitMQ connections.
 * <p>
 * Wraps {@link ConnectionFactory} from the AMQP client library and exposes
 * a fluent API for the connection settings used by the RabbitMQ transports.
 * Automatic recovery is enabled by default so the connection reconnects with
 * backoff after a broker restart.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public class RabbitMqConnectionFactory {

    private final ConnectionFactory delegate;

    /**
     * Create a factory backed by a new default {@link ConnectionFactory}.
     */
    public RabbitMqConnectionFactory() {
        this(new ConnectionFactory());
    }

    /**
     * Create a factory backed by the supplied {@link ConnectionFactory}.
     *
     * @param delegate the AMQP connection factory to wrap
     */
    public RabbitMqConnectionFactory(ConnectionFactory delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.delegate.setAutomaticRecoveryEnabled(true);
    }

    /**
     * Set the RabbitMQ broker host.
     *
     * @param host the broker host (e.g., "localhost")
     * @return this factory
     */
    public RabbitMqConnectionFactory host(String host) {
        delegate.setHost(host);
        return this;
    }

    /**
     * Set the RabbitMQ broker port.
     *
     * @param port the broker port (default 5672)
     * @return this factory
     */
    public RabbitMqConnectionFactory port(int port) {
        delegate.setPort(port);
        return this;
    }

    /**
     * Set the RabbitMQ virtual host.
     *
     * @param virtualHost the virtual host (default "/")
     * @return this factory
     */
    public RabbitMqConnectionFactory virtualHost(String virtualHost) {
        delegate.setVirtualHost(virtualHost);
        return this;
    }

    /**
     * Set the RabbitMQ user name.
     *
     * @param username the user name (default "guest")
     * @return this factory
     */
    public RabbitMqConnectionFactory username(String username) {
        delegate.setUsername(username);
        return this;
    }

    /**
     * Set the RabbitMQ password.
     *
     * @param password the password (default "guest")
     * @return this factory
     */
    public RabbitMqConnectionFactory password(String password) {
        delegate.setPassword(password);
        return this;
    }

    /**
     * Open a new connection to the RabbitMQ broker.
     *
     * @return the opened connection
     * @throws TransportException if the connection cannot be established
     */
    public Connection newConnection() {
        try {
            return delegate.newConnection();
        } catch (IOException | TimeoutException e) {
            throw new TransportException("Failed to connect to RabbitMQ", e);
        }
    }
}
