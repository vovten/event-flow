package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.outgoing.LocalQueueOutTransportFactory;
import com.github.vovten.eventflow.publisher.EventPublisher;
import com.github.vovten.eventflow.publisher.TransactionalEventPublisher;
import com.github.vovten.eventflow.transport.DefaultLocalQueueProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PublisherConfiguration}.
 */
class PublisherConfigurationTest {

    @Test
    @DisplayName("Should create EventPublisher with internal channel")
    void shouldCreateEventPublisherWithInternalChannel() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            EventFlowProperties.ChannelConfig channelConfig = new EventFlowProperties.ChannelConfig();
            channelConfig.setName("internal");
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("local-queue");
            channelConfig.getTransports().add(transportConfig);
            properties.getPublisher().getChannels().add(channelConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("testOutgoingTransportFactory", LocalQueueOutTransportFactory.class,
                    () -> new LocalQueueOutTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.register(ChannelConfiguration.class);
            context.register(PublisherConfiguration.class);
            context.refresh();

            // when
            EventPublisher publisher = context.getBean(EventPublisher.class);

            // then
            assertThat(publisher).isNotNull();
        }
    }

    @Test
    @DisplayName("Should wrap publisher with TransactionalEventPublisher when transactional is enabled")
    void shouldWrapPublisherWithTransactionalEventPublisher() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().setTransactional(true);

            EventFlowProperties.ChannelConfig channelConfig = new EventFlowProperties.ChannelConfig();
            channelConfig.setName("internal");
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("local-queue");
            channelConfig.getTransports().add(transportConfig);
            properties.getPublisher().getChannels().add(channelConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("testOutgoingTransportFactory", LocalQueueOutTransportFactory.class,
                    () -> new LocalQueueOutTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.register(ChannelConfiguration.class);
            context.register(PublisherConfiguration.class);
            context.refresh();

            // when
            EventPublisher publisher = context.getBean(EventPublisher.class);

            // then
            assertThat(publisher).isInstanceOf(TransactionalEventPublisher.class);
        }
    }

    @Test
    @DisplayName("Should not wrap publisher when transactional is disabled")
    void shouldNotWrapPublisherWhenTransactionalIsDisabled() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().setTransactional(false);

            EventFlowProperties.ChannelConfig channelConfig = new EventFlowProperties.ChannelConfig();
            channelConfig.setName("internal");
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("local-queue");
            channelConfig.getTransports().add(transportConfig);
            properties.getPublisher().getChannels().add(channelConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("testOutgoingTransportFactory", LocalQueueOutTransportFactory.class,
                    () -> new LocalQueueOutTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.register(ChannelConfiguration.class);
            context.register(PublisherConfiguration.class);
            context.refresh();

            // when
            EventPublisher publisher = context.getBean(EventPublisher.class);

            // then
            assertThat(publisher).isNotNull();
            assertThat(publisher).isNotInstanceOf(TransactionalEventPublisher.class);
        }
    }

    @Test
    @DisplayName("Should create publisher with multiple channels")
    void shouldCreatePublisherWithMultipleChannels() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();

            EventFlowProperties.ChannelConfig internalChannel = new EventFlowProperties.ChannelConfig();
            internalChannel.setName("internal");
            EventFlowProperties.TransportConfig internalTransport = new EventFlowProperties.TransportConfig();
            internalTransport.setName("local-queue");
            internalChannel.getTransports().add(internalTransport);

            EventFlowProperties.ChannelConfig customChannel = new EventFlowProperties.ChannelConfig();
            customChannel.setName("custom");
            EventFlowProperties.TransportConfig customTransport = new EventFlowProperties.TransportConfig();
            customTransport.setName("local-queue");
            customChannel.getTransports().add(customTransport);

            properties.getPublisher().getChannels().add(internalChannel);
            properties.getPublisher().getChannels().add(customChannel);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("testOutgoingTransportFactory", LocalQueueOutTransportFactory.class,
                    () -> new LocalQueueOutTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.register(ChannelConfiguration.class);
            context.register(PublisherConfiguration.class);
            context.refresh();

            // when
            EventPublisher publisher = context.getBean(EventPublisher.class);

            // then
            assertThat(publisher).isNotNull();
        }
    }

    @Test
    @DisplayName("Should not create duplicate publisher when custom bean exists")
    void shouldNotCreateDuplicatePublisherWhenCustomBeanExists() {
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

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.register(CustomPublisherConfig.class);
            context.registerBean("testOutgoingTransportFactory", LocalQueueOutTransportFactory.class,
                    () -> new LocalQueueOutTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.register(ChannelConfiguration.class);
            context.register(PublisherConfiguration.class);
            context.refresh();

            // when & then
            assertThat(context.getBean("eventPublisher")).isNotNull();
            assertThat(context.containsBean("customPublisher")).isFalse();
        }
    }

    @Configuration
    static class CustomPublisherConfig {
        @Bean(name = "eventPublisher")
        public EventPublisher customPublisher() {
            return event -> {};
        }
    }
}
