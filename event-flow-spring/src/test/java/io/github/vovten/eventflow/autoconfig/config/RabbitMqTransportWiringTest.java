package io.github.vovten.eventflow.autoconfig.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.transport.incoming.RabbitMqInTransportFactory;
import io.github.vovten.eventflow.autoconfig.transport.outgoing.RabbitMqOutTransportFactory;
import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.ExternalEventChannel;
import io.github.vovten.eventflow.dispatcher.EventDispatcher;
import io.github.vovten.eventflow.registry.EventHandlerRegistry;
import io.github.vovten.eventflow.registry.SpringEventSubscriberRegistry;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.transport.InTransport;
import io.github.vovten.eventflow.transport.rabbitmq.RabbitMqConnectionFactory;
import io.github.vovten.eventflow.transport.rabbitmq.incoming.RabbitMqInTransport;
import io.github.vovten.eventflow.transport.rabbitmq.outgoing.RabbitMqOutTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.context.support.TestPropertySourceUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Context wiring tests for RabbitMQ transports in dispatcher and channel configuration.
 * @since 1.3.0
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RabbitMqTransportWiring Tests")
class RabbitMqTransportWiringTest {

    @Mock
    private com.rabbitmq.client.ConnectionFactory amqpConnectionFactory;

    @Mock
    private Connection connection;

    @Mock
    private Channel channel;

    @Test
    @DisplayName("Should wire RabbitMqInTransport into dispatcher transports")
    void shouldWireRabbitMqInTransportIntoDispatcherTransports() throws Exception {
        // given
        stubRabbitMqConnection();
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(context,
                    "event-flow.enabled=true",
                    "event-flow.dispatcher.enabled=true");
            EventFlowProperties properties = new EventFlowProperties();
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("rabbitmq");
            transportConfig.setQueue("events");
            properties.getDispatcher().getTransports().add(transportConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(EventSerializerFactory.class, () -> new EventSerializerFactory());
            context.registerBean(RabbitMqInTransportFactory.class,
                    () -> new RabbitMqInTransportFactory(
                            context.getBean(EventSerializerFactory.class),
                            new RabbitMqConnectionFactory(amqpConnectionFactory)));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventHandlerRegistry", EventHandlerRegistry.class,
                    () -> new SpringEventSubscriberRegistry(context));
            context.register(DispatcherConfiguration.class);
            context.refresh();

            // when
            EventDispatcher dispatcher = context.getBean(EventDispatcher.class);
            List<InTransport> transports = context.getBean("dispatcherTransports", List.class);

            // then
            assertThat(dispatcher).isNotNull();
            assertThat(transports).hasSize(1);
            assertThat(transports.get(0)).isInstanceOf(RabbitMqInTransport.class);
        }
    }

    @Test
    @DisplayName("Should wire RabbitMqOutTransport into external channel")
    void shouldWireRabbitMqOutTransportIntoExternalChannel() throws Exception {
        // given
        stubRabbitMqConnection();
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(context, "event-flow.enabled=true");
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().getChannels().clear();
            EventFlowProperties.ChannelConfig channelConfig = new EventFlowProperties.ChannelConfig();
            channelConfig.setName("external");
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("rabbitmq");
            transportConfig.setQueue("events");
            channelConfig.getTransports().add(transportConfig);
            properties.getPublisher().getChannels().add(channelConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(EventSerializerFactory.class, () -> new EventSerializerFactory());
            context.registerBean(RabbitMqOutTransportFactory.class,
                    () -> new RabbitMqOutTransportFactory(
                            context.getBean(EventSerializerFactory.class),
                            new RabbitMqConnectionFactory(amqpConnectionFactory)));
            context.registerBean(SerializerConfiguration.class, () -> new SerializerConfiguration(Map.of(), properties));
            context.register(ChannelConfiguration.class);
            context.refresh();

            // when
            List<EventChannel> channels = context.getBean("eventChannels", List.class);

            // then
            assertThat(channels).hasSize(1);
            assertThat(channels.get(0)).isInstanceOf(ExternalEventChannel.class);
            assertThat(channels.get(0).transports()).hasSize(1);
            assertThat(channels.get(0).transports().get(0)).isInstanceOf(RabbitMqOutTransport.class);
        }
    }

    private void stubRabbitMqConnection() throws Exception {
        when(amqpConnectionFactory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
    }
}
