package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.outgoing.LocalQueueOutTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.outgoing.KafkaOutTransportFactory;
import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.ExternalEventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;
import com.github.vovten.eventflow.transport.DefaultLocalQueueProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ChannelConfiguration}.
 */
class ChannelConfigurationTest {

    @Test
    @DisplayName("Should create InternalEventChannel for 'internal' channel name")
    void shouldCreateInternalEventChannelForInternalChannelName() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().getChannels().clear();
            EventFlowProperties.ChannelConfig channelConfig = new EventFlowProperties.ChannelConfig();
            channelConfig.setName("internal");
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("local-queue");
            channelConfig.getTransports().add(transportConfig);
            properties.getPublisher().getChannels().add(channelConfig);

            DefaultLocalQueueProvider queueProvider = new DefaultLocalQueueProvider(1000);
            LocalQueueOutTransportFactory factory = new LocalQueueOutTransportFactory(queueProvider);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> queueProvider);
            context.registerBean("testPublisherTransportFactory1", LocalQueueOutTransportFactory.class, () -> factory);
            context.registerBean(ChannelConfiguration.class, () -> new ChannelConfiguration(properties, List.of(factory)));
            context.refresh();

            // when
            List<EventChannel> channels = context.getBean("eventChannels", List.class);

            // then
            assertThat(channels).hasSize(1);
            assertThat(channels.get(0)).isInstanceOf(InternalEventChannel.class);
        }
    }

    @Test
    @DisplayName("Should create ExternalEventChannel for 'external' channel name")
    void shouldCreateExternalEventChannelForExternalChannelName() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().getChannels().clear();
            EventFlowProperties.ChannelConfig channelConfig = new EventFlowProperties.ChannelConfig();
            channelConfig.setName("external");
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("local-queue");
            channelConfig.getTransports().add(transportConfig);
            properties.getPublisher().getChannels().add(channelConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("testPublisherTransportFactory2", LocalQueueOutTransportFactory.class,
                    () -> new LocalQueueOutTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.register(ChannelConfiguration.class);
            context.refresh();

            // when
            List<EventChannel> channels = context.getBean("eventChannels", List.class);

            // then
            assertThat(channels).hasSize(1);
            assertThat(channels.get(0)).isInstanceOf(ExternalEventChannel.class);
        }
    }

    @Test
    @DisplayName("Should create generic channel for custom channel name")
    void shouldCreateGenericChannelForCustomChannelName() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().getChannels().clear();
            EventFlowProperties.ChannelConfig channelConfig = new EventFlowProperties.ChannelConfig();
            channelConfig.setName("custom-channel");
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("local-queue");
            channelConfig.getTransports().add(transportConfig);
            properties.getPublisher().getChannels().add(channelConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("testPublisherTransportFactory3", LocalQueueOutTransportFactory.class,
                    () -> new LocalQueueOutTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.register(ChannelConfiguration.class);
            context.refresh();

            // when
            List<EventChannel> channels = context.getBean("eventChannels", List.class);

            // then
            assertThat(channels).hasSize(1);
            assertThat(channels.get(0).name()).isEqualTo("custom-channel");
        }
    }

    @Test
    @DisplayName("Should create multiple channels")
    void shouldCreateMultipleChannels() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().getChannels().clear();

            EventFlowProperties.ChannelConfig internalChannel = new EventFlowProperties.ChannelConfig();
            internalChannel.setName("internal");
            EventFlowProperties.TransportConfig internalTransport = new EventFlowProperties.TransportConfig();
            internalTransport.setName("local-queue");
            internalChannel.getTransports().add(internalTransport);

            EventFlowProperties.ChannelConfig externalChannel = new EventFlowProperties.ChannelConfig();
            externalChannel.setName("external");
            EventFlowProperties.TransportConfig externalTransport = new EventFlowProperties.TransportConfig();
            externalTransport.setName("local-queue");
            externalChannel.getTransports().add(externalTransport);

            properties.getPublisher().getChannels().add(internalChannel);
            properties.getPublisher().getChannels().add(externalChannel);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("testPublisherTransportFactory4", LocalQueueOutTransportFactory.class,
                    () -> new LocalQueueOutTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.register(ChannelConfiguration.class);
            context.refresh();

            // when
            List<EventChannel> channels = context.getBean("eventChannels", List.class);

            // then
            assertThat(channels).hasSize(2);
            assertThat(channels.get(0)).isInstanceOf(InternalEventChannel.class);
            assertThat(channels.get(1)).isInstanceOf(ExternalEventChannel.class);
        }
    }

    @Test
    @DisplayName("Should throw exception when channel has no transports")
    void shouldThrowExceptionWhenChannelHasNoTransports() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().getChannels().clear();
            EventFlowProperties.ChannelConfig channelConfig = new EventFlowProperties.ChannelConfig();
            channelConfig.setName("internal");
            // No transports

            properties.getPublisher().getChannels().add(channelConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("testPublisherTransportFactory5", LocalQueueOutTransportFactory.class,
                    () -> new LocalQueueOutTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.register(ChannelConfiguration.class);

            // when & then
            assertThatThrownBy(context::refresh)
                    .rootCause().isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must have at least one transport");
        }
    }

    @Test
    @DisplayName("Should throw exception when transport not found")
    void shouldThrowExceptionWhenTransportNotFound() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().getChannels().clear();
            EventFlowProperties.ChannelConfig channelConfig = new EventFlowProperties.ChannelConfig();
            channelConfig.setName("internal");
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("kafka");
            channelConfig.getTransports().add(transportConfig);
            properties.getPublisher().getChannels().add(channelConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("testPublisherTransportFactory6", LocalQueueOutTransportFactory.class,
                    () -> new LocalQueueOutTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.register(ChannelConfiguration.class);

            // when & then
            assertThatThrownBy(context::refresh)
                    .rootCause().isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("No factory found for transport name 'kafka'");
        }
    }

    @Test
    @DisplayName("Should use Kafka transport from configuration")
    void shouldUseKafkaTransportFromConfiguration() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().getChannels().clear();
            EventFlowProperties.ChannelConfig channelConfig = new EventFlowProperties.ChannelConfig();
            channelConfig.setName("external");
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("kafka");
            transportConfig.setTopic("test-topic");
            transportConfig.setBootstrapServers("localhost:9092");
            channelConfig.getTransports().add(transportConfig);
            properties.getPublisher().getChannels().add(channelConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean(KafkaOutTransportFactory.class,
                    () -> new KafkaOutTransportFactory());
            context.register(ChannelConfiguration.class);
            context.refresh();

            // when
            List<EventChannel> channels = context.getBean("eventChannels", List.class);

            // then
            assertThat(channels).hasSize(1);
            assertThat(channels.get(0)).isInstanceOf(ExternalEventChannel.class);
        }
    }

    @Test
    @DisplayName("Should create empty channels list when no channels configured")
    void shouldCreateEmptyChannelsListWhenNoChannelsConfigured() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().getChannels().clear();
            // No channels configured

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("testPublisherTransportFactory7", LocalQueueOutTransportFactory.class,
                    () -> new LocalQueueOutTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.register(ChannelConfiguration.class);
            context.refresh();

            // when
            List<EventChannel> channels = context.getBean("eventChannels", List.class);

            // then
            assertThat(channels).isEmpty();
        }
    }
}
