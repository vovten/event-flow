package io.github.vovten.eventflow.transport.rabbitmq;

import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Owner of a shared RabbitMQ {@link Connection} with reference counting.
 * <p>
 * A transport pair (incoming + outgoing) shares one broker connection. Each
 * transport acquires a reference when it starts to use the connection and
 * releases it when it stops. The connection is closed when the last reference
 * is released. Both {@link #acquire()} and {@link #release()} are idempotent
 * and safe to call from multiple threads.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public class RabbitMqConnectionHolder {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqConnectionHolder.class);

    private final Connection connection;
    private final AtomicInteger referenceCount = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a holder for the supplied connection.
     *
     * @param connection the shared RabbitMQ connection
     */
    public RabbitMqConnectionHolder(Connection connection) {
        this.connection = Objects.requireNonNull(connection, "connection must not be null");
    }

    /**
     * Acquire a reference to the shared connection.
     *
     * @return the shared connection
     * @throws IllegalStateException if the connection is already closed
     */
    public Connection acquire() {
        if (closed.get()) {
            throw new IllegalStateException("RabbitMQ connection is already closed");
        }
        referenceCount.incrementAndGet();
        return connection;
    }

    /**
     * Release a reference to the shared connection.
     * <p>
     * The connection is closed when the last reference is released. Calling
     * this method more often than {@link #acquire()} is safe and ignored.
     */
    public void release() {
        if (closed.get()) {
            return;
        }
        int remaining = referenceCount.updateAndGet(count -> Math.max(0, count - 1));
        if (remaining == 0 && closed.compareAndSet(false, true)) {
            closeConnection();
        }
    }

    private void closeConnection() {
        try {
            connection.close();
            log.debug("RabbitMQ connection closed");
        } catch (IOException e) {
            log.warn("Error closing RabbitMQ connection", e);
        }
    }
}
