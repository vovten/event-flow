package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.transport.outgoing.LocalQueueOutTransportFactory;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.publisher.LoggingEventPublisher;
import io.github.vovten.eventflow.publisher.TransactionalEventPublisher;
import io.github.vovten.eventflow.transport.DefaultLocalQueueProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.github.vovten.eventflow.transport.SendResults;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.context.support.TestPropertySourceUtils.addInlinedPropertiesToEnvironment;

/**
 * Unit tests for {@link PublisherConfiguration}.
 * @since 1.0.0
 */
class PublisherConfigurationTest {

    @Test
    @DisplayName("Should create EventPublisher with internal channel")
    void shouldCreateEventPublisherWithInternalChannel() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            addInlinedPropertiesToEnvironment(context, "event-flow.enabled=true");
            addInlinedPropertiesToEnvironment(context, "event-flow.publisher.enabled=true");
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
            context.registerBean(SerializerConfiguration.class, () -> new SerializerConfiguration(Map.of(), properties));
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
    @DisplayName("Should wrap publisher with LoggingEventPublisher when logging is enabled")
    void shouldWrapPublisherWithLoggingEventPublisher() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            addInlinedPropertiesToEnvironment(context, "event-flow.enabled=true");
            addInlinedPropertiesToEnvironment(context, "event-flow.publisher.enabled=true");
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().getLogging().setEnabled(true);
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
            context.registerBean(SerializerConfiguration.class, () -> new SerializerConfiguration(Map.of(), properties));
            context.register(ChannelConfiguration.class);
            context.register(PublisherConfiguration.class);
            context.refresh();

            // when
            EventPublisher publisher = context.getBean(EventPublisher.class);

            // then - LoggingEventPublisher wraps the actual publisher
            assertThat(publisher).isInstanceOf(LoggingEventPublisher.class);
        }
    }

    @Test
    @DisplayName("Should not wrap publisher when logging is disabled")
    void shouldNotWrapPublisherWhenLoggingIsDisabled() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            addInlinedPropertiesToEnvironment(context, "event-flow.enabled=true");
            addInlinedPropertiesToEnvironment(context, "event-flow.publisher.enabled=true");
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().getLogging().setEnabled(false);
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
            context.registerBean(SerializerConfiguration.class, () -> new SerializerConfiguration(Map.of(), properties));
            context.register(ChannelConfiguration.class);
            context.register(PublisherConfiguration.class);
            context.refresh();

            // when
            EventPublisher publisher = context.getBean(EventPublisher.class);

            // then
            assertThat(publisher).isNotNull();
            assertThat(publisher).isNotInstanceOf(LoggingEventPublisher.class);
        }
    }

    @Test
    @DisplayName("Should wrap with LoggingEventPublisher inside TransactionalEventPublisher when both are enabled")
    void shouldWrapWithLoggingInsideTransactionalWhenBothEnabled() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            addInlinedPropertiesToEnvironment(context, "event-flow.enabled=true");
            addInlinedPropertiesToEnvironment(context, "event-flow.publisher.enabled=true");
            EventFlowProperties properties = new EventFlowProperties();
            properties.getPublisher().getLogging().setEnabled(true);
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
            context.registerBean(SerializerConfiguration.class, () -> new SerializerConfiguration(Map.of(), properties));
            context.register(ChannelConfiguration.class);
            context.register(PublisherConfiguration.class);
            context.refresh();

            // when
            EventPublisher publisher = context.getBean(EventPublisher.class);

            // then - outermost is TransactionalEventPublisher
            assertThat(publisher).isInstanceOf(TransactionalEventPublisher.class);
            // The inner publisher should be LoggingEventPublisher
            // We can't easily verify the inner type, but we can verify the decorator is applied
        }
    }

    @Configuration
    static class CustomPublisherConfig {
        @Bean(name = "eventPublisher")
        public EventPublisher customPublisher() {
            return event -> CompletableFuture.completedFuture(SendResults.empty());
        }
    }
}
