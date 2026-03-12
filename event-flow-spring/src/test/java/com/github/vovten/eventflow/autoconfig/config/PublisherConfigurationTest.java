package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.InMemoryOutgoingTransportFactory;
import com.github.vovten.eventflow.publisher.EventPublisher;
import com.github.vovten.eventflow.publisher.TransactionalEventPublisher;
import com.github.vovten.eventflow.transport.DefaultQueueProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            channelConfig.getTransports().add(new EventFlowProperties.TransportRef("default", "in-memory", new EventFlowProperties.TransportConfig()));
            properties.getPublisher().getChannels().add(channelConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultQueueProvider.class, () -> new DefaultQueueProvider(1000));
            context.register(InMemoryOutgoingTransportFactory.class);
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
            channelConfig.getTransports().add(new EventFlowProperties.TransportRef("default", "in-memory", new EventFlowProperties.TransportConfig()));
            properties.getPublisher().getChannels().add(channelConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultQueueProvider.class, () -> new DefaultQueueProvider(1000));
            context.register(InMemoryOutgoingTransportFactory.class);
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
            channelConfig.getTransports().add(new EventFlowProperties.TransportRef("default", "in-memory", new EventFlowProperties.TransportConfig()));
            properties.getPublisher().getChannels().add(channelConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultQueueProvider.class, () -> new DefaultQueueProvider(1000));
            context.register(InMemoryOutgoingTransportFactory.class);
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
            internalChannel.getTransports().add(new EventFlowProperties.TransportRef("default", "in-memory", new EventFlowProperties.TransportConfig()));

            EventFlowProperties.ChannelConfig customChannel = new EventFlowProperties.ChannelConfig();
            customChannel.setName("custom");
            customChannel.getTransports().add(new EventFlowProperties.TransportRef("custom", "in-memory", new EventFlowProperties.TransportConfig()));

            properties.getPublisher().getChannels().add(internalChannel);
            properties.getPublisher().getChannels().add(customChannel);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultQueueProvider.class, () -> new DefaultQueueProvider(1000));
            context.register(InMemoryOutgoingTransportFactory.class);
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
    @DisplayName("Should throw exception when channel has no transports")
    void shouldThrowExceptionWhenChannelHasNoTransports() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();

            EventFlowProperties.ChannelConfig channelConfig = new EventFlowProperties.ChannelConfig();
            channelConfig.setName("internal");
            // No transports added
            
            properties.getPublisher().getChannels().add(channelConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultQueueProvider.class, () -> new DefaultQueueProvider(1000));
            context.register(InMemoryOutgoingTransportFactory.class);
            context.register(ChannelConfiguration.class);
            context.register(PublisherConfiguration.class);

            // when & then
            assertThatThrownBy(context::refresh)
                    .rootCause().isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must have at least one transport");
        }
    }

    @Test
    @DisplayName("Should not create duplicate publisher when custom bean exists")
    void shouldNotCreateDuplicatePublisherWhenCustomBeanExists() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();

            EventFlowProperties.ChannelConfig channelConfig = new EventFlowProperties.ChannelConfig();
            channelConfig.setName("internal");
            channelConfig.getTransports().add(new EventFlowProperties.TransportRef("default", "in-memory", new EventFlowProperties.TransportConfig()));
            properties.getPublisher().getChannels().add(channelConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultQueueProvider.class, () -> new DefaultQueueProvider(1000));
            context.register(InMemoryOutgoingTransportFactory.class);
            context.register(CustomPublisherConfig.class);
            context.register(ChannelConfiguration.class);
            context.register(PublisherConfiguration.class);
            context.refresh();

            // when & then
            assertThat(context.getBean("customPublisher")).isNotNull();
            assertThat(context.containsBean("eventPublisher")).isFalse();
        }
    }

    @Configuration
    static class CustomPublisherConfig {
        @Bean(name = "customPublisher")
        public EventPublisher customPublisher() {
            return event -> {};
        }
    }
}
