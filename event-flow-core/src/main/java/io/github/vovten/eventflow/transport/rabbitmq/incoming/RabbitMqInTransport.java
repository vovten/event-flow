package io.github.vovten.eventflow.transport.rabbitmq.incoming;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.InTransport;
import io.github.vovten.eventflow.transport.TransportException;
import io.github.vovten.eventflow.transport.rabbitmq.RabbitMqConnectionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * RabbitMQ incoming transport for receiving external events.
 * <p>
 * On {@link #start(Consumer)} the transport opens a channel, declares the
 * configured queue (durable=false by default) and registers a consumer that
 * deserializes the message body via {@link EventSerializerFactory} (magic-byte
 * detection, same as the Kafka incoming transport) and forwards the event to
 * the consumer callback. Start is idempotent; stop cancels the consumer and
 * closes the channel.
 * <p>
 * The transport shares one broker connection with its outgoing counterpart via
 * a {@link RabbitMqConnectionHolder}. A reference is acquired when the
 * transport is created and released on stop or close, so the connection closes
 * when both transports of the pair have released their references.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 * @see EventSerializerFactory
 */
public class RabbitMqInTransport implements InTransport, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqInTransport.class);

    private final Connection connection;
    private final String queue;
    private final boolean queueDurable;
    private final EventSerializerFactory serializerFactory;
    private final RabbitMqConnectionHolder connectionHolder;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean connectionReleased = new AtomicBoolean(false);

    private volatile Channel channel;
    private volatile String consumerTag;

    /**
     * Create a RabbitMQ incoming transport for the supplied queue.
     *
     * @param connection        the shared broker connection
     * @param queue             the queue name to consume from
     * @param queueDurable      whether the queue should be declared durable
     * @param serializerFactory the serializer factory for magic-byte detection
     * @param connectionHolder  the shared connection owner
     */
    public RabbitMqInTransport(Connection connection, String queue, boolean queueDurable,
                               EventSerializerFactory serializerFactory,
                               RabbitMqConnectionHolder connectionHolder) {
        this.connection = Objects.requireNonNull(connection, "connection must not be null");
        this.queue = Objects.requireNonNull(queue, "queue must not be null");
        this.queueDurable = queueDurable;
        this.serializerFactory = Objects.requireNonNull(serializerFactory, "serializerFactory must not be null");
        this.connectionHolder = Objects.requireNonNull(connectionHolder, "connectionHolder must not be null");
        connectionHolder.acquire();
    }

    @Override
    public String name() {
        return "rabbitmq";
    }

    @Override
    public void start(Consumer<Event> eventConsumer) {
        if (running.compareAndSet(false, true)) {
            try {
                Channel newChannel = openChannel();
                declareQueue(newChannel);
                this.channel = newChannel;
                this.consumerTag = registerConsumer(newChannel, eventConsumer);
                log.info("RabbitMqInTransport started, consuming from queue '{}'", queue);
            } catch (RuntimeException e) {
                running.set(false);
                releaseFailedStartResources();
                throw e;
            }
        } else {
            log.warn("RabbitMqInTransport is already running");
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            cancelConsumer();
            closeChannel();
            releaseConnection();
            log.info("RabbitMqInTransport stopped, queue '{}'", queue);
        }
    }

    /**
     * Release all resources held by this transport.
     * <p>
     * Stops the consumer if the transport is running, closes the channel and
     * releases the shared connection reference. Safe to call multiple times.
     */
    @Override
    public void close() {
        if (running.get()) {
            stop();
        } else {
            releaseConnection();
        }
    }

    private Channel openChannel() {
        try {
            return connection.createChannel();
        } catch (Exception e) {
            throw new TransportException("Failed to open RabbitMQ channel for queue '" + queue + "'", e);
        }
    }

    private void declareQueue(Channel newChannel) {
        try {
            newChannel.queueDeclare(queue, queueDurable, false, false, null);
        } catch (Exception e) {
            throw new TransportException("Failed to declare RabbitMQ queue '" + queue + "'", e);
        }
    }

    private String registerConsumer(Channel newChannel, Consumer<Event> eventConsumer) {
        try {
            return newChannel.basicConsume(queue, true, createDeliverCallback(eventConsumer), createCancelCallback());
        } catch (Exception e) {
            throw new TransportException("Failed to register RabbitMQ consumer for queue '" + queue + "'", e);
        }
    }

    private DeliverCallback createDeliverCallback(Consumer<Event> eventConsumer) {
        return (consumerTag, delivery) -> tryDeliver(delivery, eventConsumer);
    }

    private com.rabbitmq.client.CancelCallback createCancelCallback() {
        return cancelledTag -> log.warn("RabbitMQ consumer cancelled for queue '{}'", queue);
    }

    private void tryDeliver(Delivery delivery, Consumer<Event> eventConsumer) {
        try {
            byte[] body = delivery.getBody();
            EventSerializer serializer = serializerFactory.getByData(body);
            Event event = serializer.deserialize(body, Event.class);
            MDC.put("deliveredFrom", name() + "-" + queue);
            eventConsumer.accept(event);
            if (log.isDebugEnabled()) {
                log.debug("Event delivered from RabbitMQ queue '{}'", queue);
            }
        } catch (Exception e) {
            log.error("Failed to deliver event from RabbitMQ queue '{}'", queue, e);
        } finally {
            MDC.remove("deliveredFrom");
        }
    }

    private void cancelConsumer() {
        if (consumerTag != null) {
            try {
                channel.basicCancel(consumerTag);
            } catch (Exception e) {
                log.warn("Error cancelling RabbitMQ consumer for queue '{}'", queue, e);
            }
        }
    }

    private void closeChannel() {
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception e) {
                log.warn("Error closing RabbitMQ channel for queue '{}'", queue, e);
            }
        }
    }

    private void releaseFailedStartResources() {
        cancelConsumer();
        closeChannel();
        releaseConnection();
    }

    private void releaseConnection() {
        if (connectionReleased.compareAndSet(false, true)) {
            connectionHolder.release();
        }
    }
}
