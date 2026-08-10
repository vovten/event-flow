package io.github.vovten.eventflow.autoconfig.transport.outgoing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.rabbitmq.RabbitMqConnectionFactory;
import io.github.vovten.eventflow.transport.rabbitmq.outgoing.RabbitMqOutTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for RabbitMQ publisher transport factory.
 * @since 1.3.0
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RabbitMqOutTransportFactory Tests")
class RabbitMqOutTransportFactoryTest {

    @Mock
    private com.rabbitmq.client.ConnectionFactory amqpConnectionFactory;

    @Mock
    private Connection connection;

    @Mock
    private Channel channel;

    private final EventSerializerFactory serializerFactory = new EventSerializerFactory();

    @Test
    @DisplayName("RabbitMqOutTransportFactory should have correct name")
    void rabbitMqOutTransportFactoryShouldHaveCorrectName() {
        // given
        RabbitMqOutTransportFactory factory = new RabbitMqOutTransportFactory(serializerFactory);

        // when
        String name = factory.getName();

        // then
        assertThat(name).isEqualTo("rabbitmq");
    }

    @Test
    @DisplayName("RabbitMqOutTransportFactory should create publisher transport")
    void rabbitMqOutTransportFactoryShouldCreatePublisherTransport() throws Exception {
        // given
        stubConnection();
        RabbitMqOutTransportFactory factory = new RabbitMqOutTransportFactory(serializerFactory,
                new RabbitMqConnectionFactory(amqpConnectionFactory));
        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("rabbitmq");
        config.setQueue("test-queue");

        // when
        OutTransport transport = factory.createPublisher(config);

        // then
        assertThat(transport).isInstanceOf(RabbitMqOutTransport.class);
    }

    @Test
    @DisplayName("RabbitMqOutTransportFactory should apply connection settings from config")
    void rabbitMqOutTransportFactoryShouldApplyConnectionSettings() throws Exception {
        // given
        stubConnection();
        RabbitMqOutTransportFactory factory = new RabbitMqOutTransportFactory(serializerFactory,
                new RabbitMqConnectionFactory(amqpConnectionFactory));
        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("rabbitmq");
        config.setQueue("test-queue");
        config.setHost("rabbit.example.com");
        config.setPort(5673);
        config.setVirtualHost("orders");
        config.setUsername("app-user");
        config.setPassword("secret");

        // when
        factory.createPublisher(config);

        // then
        verify(amqpConnectionFactory).setHost("rabbit.example.com");
        verify(amqpConnectionFactory).setPort(5673);
        verify(amqpConnectionFactory).setVirtualHost("orders");
        verify(amqpConnectionFactory).setUsername("app-user");
        verify(amqpConnectionFactory).setPassword("secret");
    }

    @Test
    @DisplayName("RabbitMqOutTransportFactory should throw exception when queue is missing")
    void rabbitMqOutTransportFactoryShouldThrowExceptionWhenQueueIsMissing() {
        // given
        RabbitMqOutTransportFactory factory = new RabbitMqOutTransportFactory(serializerFactory);
        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("rabbitmq");

        // when & then
        assertThatThrownBy(() -> factory.validate(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RabbitMQ transport requires 'queue' configuration");
    }

    private void stubConnection() throws Exception {
        when(amqpConnectionFactory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
    }
}
