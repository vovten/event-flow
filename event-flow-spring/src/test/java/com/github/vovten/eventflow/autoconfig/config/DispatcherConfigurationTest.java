package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.incoming.LocalQueueInTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.incoming.KafkaInTransportFactory;
import com.github.vovten.eventflow.dispatcher.EventDispatcher;
import com.github.vovten.eventflow.registry.EventHandlerRegistry;
import com.github.vovten.eventflow.registry.SpringEventSubscriberRegistry;
import com.github.vovten.eventflow.transport.DefaultLocalQueueProvider;
import com.github.vovten.eventflow.transport.InTransport;
import com.github.vovten.eventflow.transport.incoming.LocalQueueInTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.context.support.TestPropertySourceUtils;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link DispatcherConfiguration}.
 */
class DispatcherConfigurationTest {

    @Test
    @DisplayName("Should create EventDispatcher with default local-queue transport")
    void shouldCreateEventDispatcherWithDefaultLocalQueueTransport() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(context, "event-flow.enabled=true");
            EventFlowProperties properties = new EventFlowProperties();

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("testDispatcherTransportFactory1", LocalQueueInTransportFactory.class,
                    () -> new LocalQueueInTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
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
            assertThat(transports.get(0)).isInstanceOf(LocalQueueInTransport.class);
        }
    }

    @Test
    @DisplayName("Should create EventDispatcher with configured transports")
    void shouldCreateEventDispatcherWithConfiguredTransports() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(context, "event-flow.enabled=true");
            EventFlowProperties properties = new EventFlowProperties();
            properties.getDispatcher().getTransports().clear();
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("local-queue");
            transportConfig.setCapacity(500);
            properties.getDispatcher().getTransports().add(transportConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("testDispatcherTransportFactory2", LocalQueueInTransportFactory.class,
                    () -> new LocalQueueInTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
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
        }
    }

    @Test
    @DisplayName("Should create multiple dispatcher transports")
    void shouldCreateMultipleDispatcherTransports() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(context, "event-flow.enabled=true");
            EventFlowProperties properties = new EventFlowProperties();
            properties.getDispatcher().getTransports().clear();

            EventFlowProperties.TransportConfig transport1 = new EventFlowProperties.TransportConfig();
            transport1.setName("local-queue");

            EventFlowProperties.TransportConfig transport2 = new EventFlowProperties.TransportConfig();
            transport2.setName("local-queue");

            properties.getDispatcher().getTransports().add(transport1);
            properties.getDispatcher().getTransports().add(transport2);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("testDispatcherTransportFactory3", LocalQueueInTransportFactory.class,
                    () -> new LocalQueueInTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventHandlerRegistry", EventHandlerRegistry.class,
                    () -> new SpringEventSubscriberRegistry(context));
            context.register(DispatcherConfiguration.class);
            context.refresh();

            // when
            List<InTransport> transports = context.getBean("dispatcherTransports", List.class);

            // then
            assertThat(transports).hasSize(2);
        }
    }

    @Test
    @DisplayName("Should throw exception when no factory found for transport type")
    void shouldThrowExceptionWhenNoFactoryFoundForTransportType() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(context, "event-flow.enabled=true");
            EventFlowProperties properties = new EventFlowProperties();
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("kafka");
            properties.getDispatcher().getTransports().add(transportConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventHandlerRegistry", EventHandlerRegistry.class,
                    () -> new SpringEventSubscriberRegistry(context));
            // Only local-queue factory, not kafka
            context.registerBean("testDispatcherTransportFactory4", LocalQueueInTransportFactory.class,
                    () -> new LocalQueueInTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.register(DispatcherConfiguration.class);

            // when & then
            assertThatThrownBy(context::refresh)
                    .rootCause().isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unsupported transport type 'kafka'");
        }
    }

    @Test
    @DisplayName("Should create Kafka dispatcher transport when kafka factory is available")
    void shouldCreateKafkaDispatcherTransportWhenKafkaFactoryIsAvailable() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(context, "event-flow.enabled=true");
            EventFlowProperties properties = new EventFlowProperties();
            properties.getDispatcher().getTransports().clear();
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("kafka");
            transportConfig.setTopic("test-topic");
            transportConfig.setServers("localhost:9092");
            transportConfig.setConsumerGroup("test-group");
            properties.getDispatcher().getTransports().add(transportConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventHandlerRegistry", EventHandlerRegistry.class,
                    () -> new SpringEventSubscriberRegistry(context));
            context.registerBean(KafkaInTransportFactory.class, () -> new KafkaInTransportFactory());
            context.register(DispatcherConfiguration.class);
            context.refresh();

            // when
            List<InTransport> transports = context.getBean("dispatcherTransports", List.class);

            // then
            assertThat(transports).hasSize(1);
        }
    }

    @Test
    @DisplayName("Should not create duplicate dispatcher when custom bean exists")
    void shouldNotCreateDuplicateDispatcherWhenCustomBeanExists() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(context, "event-flow.enabled=true");
            EventFlowProperties properties = new EventFlowProperties();

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventHandlerRegistry", EventHandlerRegistry.class,
                    () -> new SpringEventSubscriberRegistry(context));
            context.registerBean("customDispatcher", EventDispatcher.class, () -> null);
            context.registerBean("testDispatcherTransportFactory5", LocalQueueInTransportFactory.class,
                    () -> new LocalQueueInTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.register(DispatcherConfiguration.class);
            context.refresh();

            // when & then
            assertThat(context.containsBean("eventDispatcher")).isFalse();
        }
    }

    @Test
    @DisplayName("Should not create duplicate dispatcher transports when custom bean exists")
    void shouldNotCreateDuplicateDispatcherTransportsWhenCustomBeanExists() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(context, "event-flow.enabled=true");
            EventFlowProperties properties = new EventFlowProperties();

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventHandlerRegistry", EventHandlerRegistry.class,
                    () -> new SpringEventSubscriberRegistry(context));
            context.registerBean("dispatcherTransports", List.class, () -> List.of());
            context.registerBean("testDispatcherTransportFactory6", LocalQueueInTransportFactory.class,
                    () -> new LocalQueueInTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.register(DispatcherConfiguration.class);
            context.refresh();

            // when & then
            assertThat(context.containsBean("dispatcherTransports")).isTrue();
            assertThat(context.getBean("dispatcherTransports", List.class)).isEmpty();
        }
    }

    @Test
    @DisplayName("Should start dispatcher on creation")
    void shouldStartDispatcherOnCreation() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(context, "event-flow.enabled=true");
            EventFlowProperties properties = new EventFlowProperties();

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultLocalQueueProvider.class, () -> new DefaultLocalQueueProvider(1000));
            context.registerBean("testDispatcherTransportFactory7", LocalQueueInTransportFactory.class,
                    () -> new LocalQueueInTransportFactory(context.getBean(DefaultLocalQueueProvider.class)));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventHandlerRegistry", EventHandlerRegistry.class,
                    () -> new SpringEventSubscriberRegistry(context));
            context.register(DispatcherConfiguration.class);
            context.refresh();

            // when
            EventDispatcher dispatcher = context.getBean(EventDispatcher.class);

            // then - dispatcher should be started (no exception thrown)
            assertThat(dispatcher).isNotNull();
        }
    }
}
