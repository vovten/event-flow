package io.github.vovten.eventflow.transport.incoming.rabbitmq;

import com.rabbitmq.client.*;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.InTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RabbitMQ incoming transport for receiving external events.
 * <p>
 * Listens to a RabbitMQ queue and delivers events to the registered consumer.
 * Automatically detects serialization format (JSON, MessagePack).
 * <p>
 * Example:
 * <pre>{@code
 * try (RabbitMqInTransport transport = new RabbitMqInTransport("amqp://localhost:5672", "events")) {
 *     transport.start(event -> System.out.println("Received: " + event));
 *     Thread.sleep(60000);
 * }
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 */
public class RabbitMqInTransport implements InTransport, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqInTransport.class);

    private final Connection connection;
    private Channel channel;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final EventSerializerFactory serializerFactory;
    private final String queue;
    private final String exchange;
    private final String routingKey;
    private String consumerTag;

    /**
     * Create RabbitMQ transport with connection URI.
     *
     * @param uri   RabbitMQ connection URI (e.g., "amqp://localhost:5672")
     * @param queue queue name to consume from
     */
    public RabbitMqInTransport(String uri, String queue) {
        this(uri, "", "events", queue, new EventSerializerFactory());
    }

    /**
     * Create RabbitMQ transport with exchange and queue binding.
     *
     * @param uri        RabbitMQ connection URI
     * @param exchange   exchange name
     * @param routingKey routing key for binding
     * @param queue      queue name
     */
    public RabbitMqInTransport(String uri, String exchange, String routingKey, String queue) {
        this(uri, exchange, routingKey, queue, new EventSerializerFactory());
    }

    /**
     * Create RabbitMQ transport with custom serializer factory.
     *
     * @param uri              RabbitMQ connection URI
     * @param exchange         exchange name
     * @param routingKey       routing key for binding
     * @param queue            queue name
     * @param serializerFactory serializer factory for event deserialization
     */
    public RabbitMqInTransport(String uri, String exchange, String routingKey,
                                String queue, EventSerializerFactory serializerFactory) {
        this.serializerFactory = serializerFactory;
        this.queue = Objects.requireNonNull(queue, "Queue must not be null");
        this.exchange = exchange;
        this.routingKey = routingKey;

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(Objects.requireNonNull(uri, "URI must not be null"));

            this.connection = factory.newConnection();
            this.channel = connection.createChannel();
            

            // Declare exchange if provided
            if (exchange != null && !exchange.isEmpty()) {
                channel.exchangeDeclare(exchange, "direct", true);
                channel.queueDeclare(queue, true, false, false, null);
                channel.queueBind(queue, exchange, routingKey);
            } else {
                channel.queueDeclare(queue, true, false, false, null);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create RabbitMQ connection", e);
        }

        this.executorService = Executors.newSingleThreadExecutor();
    }

    @Override
    public String name() {
        return "rabbitmq";
    }

    @Override
    public void start(java.util.function.Consumer<Event> eventConsumer) {
        if (running.compareAndSet(false, true)) {
            executorService.execute(() -> consumeLoop(eventConsumer));
            log.info("RabbitMqInTransport started, listening to queue: {}", queue);
        } else {
            log.warn("RabbitMqInTransport is already running");
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping RabbitMqInTransport...");
            try {
                if (channel != null && channel.isOpen()) {
                    if (consumerTag != null) {
                        channel.basicCancel(consumerTag);
                    }
                    channel.close();
                }
                if (connection != null && connection.isOpen()) {
                    connection.close();
                }
            } catch (Exception e) {
                log.warn("Error stopping RabbitMQ transport", e);
            }
            executorService.shutdownNow();
            log.info("RabbitMqInTransport stopped");
        }
    }

    @Override
    public void close() {
        stop();
    }

    private void consumeLoop(java.util.function.Consumer<Event> eventConsumer) {
        try {
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                tryDeliverEvent(delivery, eventConsumer);
            };

            CancelCallback cancelCallback = consumerTag -> {
                log.info("Consumer cancelled: {}", consumerTag);
            };

            consumerTag = channel.basicConsume(queue, false, deliverCallback, cancelCallback);

            while (running.get()) {
                Thread.sleep(100);
            }
        } catch (Exception e) {
            if (running.get()) {
                log.error("RabbitMqInTransport loop error", e);
            }
        }
    }

    private void tryDeliverEvent(Delivery delivery, java.util.function.Consumer<Event> eventConsumer) {
        try {
            byte[] data = delivery.getBody();
            EventSerializer serializer = serializerFactory.getByData(data);
            Event event = serializer.deserialize(data, Event.class);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            eventConsumer.accept(event);
            if (log.isDebugEnabled()) {
                log.debug("Event delivered from RabbitMQ queue: {}, routing key: {}",
                        queue, delivery.getEnvelope().getRoutingKey());
            }
        } catch (Exception e) {
            log.error("Failed to deliver event from queue: {}", queue, e);
            try {
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            } catch (IOException ex) {
                log.error("Failed to nack message", ex);
            }
        }
    }
}
