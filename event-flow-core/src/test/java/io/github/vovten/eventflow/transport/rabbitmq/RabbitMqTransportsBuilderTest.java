package io.github.vovten.eventflow.transport.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.transport.rabbitmq.incoming.RabbitMqInTransport;
import io.github.vovten.eventflow.transport.rabbitmq.outgoing.RabbitMqOutTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RabbitMqTransportsBuilder}.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RabbitMqTransportsBuilder Tests")
class RabbitMqTransportsBuilderTest {

    @Mock
    private com.rabbitmq.client.ConnectionFactory amqpConnectionFactory;

    @Mock
    private Connection connection;

    @Mock
    private Channel channel;

    @Test
    @DisplayName("Should build dispatcher and publisher transports")
    void shouldBuildDispatcherAndPublisherTransports() throws IOException {
        // given
        stubConnection();

        // when
        RabbitMqTransportsBuilder.RabbitMqTransports transports = builderWithDefaultQueue().build();

        // then
        assertThat(transports.dispatcher()).isInstanceOf(RabbitMqInTransport.class);
        assertThat(transports.publisher()).isInstanceOf(RabbitMqOutTransport.class);
        assertThat(transports.dispatcher().name()).isEqualTo("rabbitmq");
        assertThat(transports.publisher().name()).isEqualTo("rabbitmq");

        transports.publisher().close();
    }

    @Test
    @DisplayName("Should share a single connection between the transports")
    void shouldShareSingleConnectionBetweenTransports() throws Exception {
        // given
        stubConnection();

        // when
        RabbitMqTransportsBuilder.RabbitMqTransports transports = builderWithDefaultQueue().build();

        // then
        verify(amqpConnectionFactory, times(1)).newConnection();

        transports.publisher().close();
    }

    @Test
    @DisplayName("Should apply custom connection properties")
    void shouldApplyCustomConnectionProperties() throws IOException {
        // given
        stubConnection();

        // when
        RabbitMqTransportsBuilder.RabbitMqTransports transports = new RabbitMqTransportsBuilder()
                .connectionFactory(new RabbitMqConnectionFactory(amqpConnectionFactory))
                .host("rabbit.example.com")
                .port(5673)
                .virtualHost("orders")
                .username("app-user")
                .password("secret")
                .queue("events")
                .build();

        // then
        verify(amqpConnectionFactory).setHost("rabbit.example.com");
        verify(amqpConnectionFactory).setPort(5673);
        verify(amqpConnectionFactory).setVirtualHost("orders");
        verify(amqpConnectionFactory).setUsername("app-user");
        verify(amqpConnectionFactory).setPassword("secret");

        transports.publisher().close();
    }

    @Test
    @DisplayName("Should default routing key to the queue name")
    void shouldDefaultRoutingKeyToQueueName() throws IOException {
        // given
        stubConnection();
        RabbitMqTransportsBuilder.RabbitMqTransports transports = new RabbitMqTransportsBuilder()
                .connectionFactory(new RabbitMqConnectionFactory(amqpConnectionFactory))
                .queue("my-queue")
                .build();

        // when
        transports.publisher().send(TestEvent.create("id-1", "message-1"));

        // then
        verify(channel).basicPublish(eq(""), eq("my-queue"), isNull(), any(byte[].class));

        transports.publisher().close();
    }

    @Test
    @DisplayName("Should publish to the configured exchange with the configured routing key")
    void shouldPublishToConfiguredExchangeWithRoutingKey() throws IOException {
        // given
        stubConnection();
        RabbitMqTransportsBuilder.RabbitMqTransports transports = new RabbitMqTransportsBuilder()
                .connectionFactory(new RabbitMqConnectionFactory(amqpConnectionFactory))
                .queue("my-queue")
                .exchange("my-exchange")
                .routingKey("my-key")
                .build();

        // when
        transports.publisher().send(TestEvent.create("id-2", "message-2"));

        // then
        verify(channel).basicPublish(eq("my-exchange"), eq("my-key"), isNull(), any(byte[].class));

        transports.publisher().close();
    }

    @Test
    @DisplayName("Should declare the queue with configured durability on dispatcher start")
    void shouldDeclareQueueWithConfiguredDurability() throws IOException {
        // given
        stubConnection();
        RabbitMqTransportsBuilder.RabbitMqTransports transports = new RabbitMqTransportsBuilder()
                .connectionFactory(new RabbitMqConnectionFactory(amqpConnectionFactory))
                .queue("events")
                .queueDurable(true)
                .build();

        // when
        transports.dispatcher().start(event -> { });

        // then
        verify(channel).queueDeclare("events", true, false, false, null);

        transports.dispatcher().stop();
    }

    private void stubConnection() {
        try {
            when(amqpConnectionFactory.newConnection()).thenReturn(connection);
            when(connection.createChannel()).thenReturn(channel);
        } catch (IOException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Failed to stub RabbitMQ connection", e);
        }
    }

    private RabbitMqTransportsBuilder builderWithDefaultQueue() {
        return new RabbitMqTransportsBuilder()
                .connectionFactory(new RabbitMqConnectionFactory(amqpConnectionFactory));
    }

    /**
     * Test event class.
     */
    static class TestEvent extends AbstractTraceableEvent {
        public String id;
        public String message;

        TestEvent() {
            super();
        }

        TestEvent(String id, String message) {
            super();
            this.id = id;
            this.message = message;
        }

        static TestEvent create(String id, String message) {
            return new TestEvent(id, message);
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }
}
