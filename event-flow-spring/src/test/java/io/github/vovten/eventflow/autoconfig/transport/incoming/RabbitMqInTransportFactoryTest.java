package io.github.vovten.eventflow.autoconfig.transport.incoming;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.InTransport;
import io.github.vovten.eventflow.transport.rabbitmq.RabbitMqConnectionFactory;
import io.github.vovten.eventflow.transport.rabbitmq.incoming.RabbitMqInTransport;
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
 * Unit tests for RabbitMQ dispatcher transport factory.
 * @since 1.3.0
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RabbitMqInTransportFactory Tests")
class RabbitMqInTransportFactoryTest {

    @Mock
    private com.rabbitmq.client.ConnectionFactory amqpConnectionFactory;

    @Mock
    private Connection connection;

    @Mock
    private Channel channel;

    private final EventSerializerFactory serializerFactory = new EventSerializerFactory();

    @Test
    @DisplayName("RabbitMqInTransportFactory should have correct type")
    void rabbitMqInTransportFactoryShouldHaveCorrectType() {
        // given
        RabbitMqInTransportFactory factory = new RabbitMqInTransportFactory(serializerFactory);

        // when
        String type = factory.getType();

        // then
        assertThat(type).isEqualTo("rabbitmq");
    }

    @Test
    @DisplayName("RabbitMqInTransportFactory should create dispatcher transport")
    void rabbitMqInTransportFactoryShouldCreateDispatcherTransport() throws Exception {
        // given
        stubConnection();
        RabbitMqInTransportFactory factory = new RabbitMqInTransportFactory(serializerFactory,
                new RabbitMqConnectionFactory(amqpConnectionFactory));
        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("rabbitmq");
        config.setQueue("test-queue");

        // when
        InTransport transport = factory.createDispatcher(config);

        // then
        assertThat(transport).isInstanceOf(RabbitMqInTransport.class);
    }

    @Test
    @DisplayName("RabbitMqInTransportFactory should apply connection settings from config")
    void rabbitMqInTransportFactoryShouldApplyConnectionSettings() throws Exception {
        // given
        stubConnection();
        RabbitMqInTransportFactory factory = new RabbitMqInTransportFactory(serializerFactory,
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
        factory.createDispatcher(config);

        // then
        verify(amqpConnectionFactory).setHost("rabbit.example.com");
        verify(amqpConnectionFactory).setPort(5673);
        verify(amqpConnectionFactory).setVirtualHost("orders");
        verify(amqpConnectionFactory).setUsername("app-user");
        verify(amqpConnectionFactory).setPassword("secret");
    }

    @Test
    @DisplayName("RabbitMqInTransportFactory should throw exception when queue is missing")
    void rabbitMqInTransportFactoryShouldThrowExceptionWhenQueueIsMissing() {
        // given
        RabbitMqInTransportFactory factory = new RabbitMqInTransportFactory(serializerFactory);
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
