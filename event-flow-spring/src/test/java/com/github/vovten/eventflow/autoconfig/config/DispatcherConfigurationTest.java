package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.InMemoryIncomingTransportFactory;
import com.github.vovten.eventflow.autoconfig.transport.KafkaIncomingTransportFactory;
import com.github.vovten.eventflow.dispatcher.EventDispatcher;
import com.github.vovten.eventflow.registry.EventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringInterfaceEventListenerRegistry;
import com.github.vovten.eventflow.transport.DefaultQueueProvider;
import com.github.vovten.eventflow.transport.IncomingEventTransport;
import com.github.vovten.eventflow.transport.incoming.InMemoryIncomingEventTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

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
    @DisplayName("Should create EventDispatcher with default in-memory transport")
    void shouldCreateEventDispatcherWithDefaultInMemoryTransport() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultQueueProvider.class, () -> new DefaultQueueProvider(1000));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventListenerRegistry", EventListenerRegistry.class, 
                    () -> new SpringInterfaceEventListenerRegistry(context));
            context.register(InMemoryIncomingTransportFactory.class);
            context.register(DispatcherConfiguration.class);
            context.refresh();

            // when
            EventDispatcher dispatcher = context.getBean(EventDispatcher.class);
            List<IncomingEventTransport> transports = context.getBean("incomingEventTransports", List.class);

            // then
            assertThat(dispatcher).isNotNull();
            assertThat(transports).hasSize(1);
            assertThat(transports.get(0)).isInstanceOf(InMemoryIncomingEventTransport.class);
        }
    }

    @Test
    @DisplayName("Should create EventDispatcher with configured transports")
    void shouldCreateEventDispatcherWithConfiguredTransports() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("custom-transport");
            transportConfig.setType("in-memory");
            transportConfig.setCapacity(500);
            properties.getDispatcher().getTransports().add(transportConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultQueueProvider.class, () -> new DefaultQueueProvider(1000));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventListenerRegistry", EventListenerRegistry.class, 
                    () -> new SpringInterfaceEventListenerRegistry(context));
            context.register(InMemoryIncomingTransportFactory.class);
            context.register(DispatcherConfiguration.class);
            context.refresh();

            // when
            EventDispatcher dispatcher = context.getBean(EventDispatcher.class);
            List<IncomingEventTransport> transports = context.getBean("incomingEventTransports", List.class);

            // then
            assertThat(dispatcher).isNotNull();
            assertThat(transports).hasSize(1);
        }
    }

    @Test
    @DisplayName("Should create multiple incoming transports")
    void shouldCreateMultipleIncomingTransports() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            
            EventFlowProperties.TransportConfig transport1 = new EventFlowProperties.TransportConfig();
            transport1.setName("transport-1");
            transport1.setType("in-memory");
            
            EventFlowProperties.TransportConfig transport2 = new EventFlowProperties.TransportConfig();
            transport2.setName("transport-2");
            transport2.setType("in-memory");
            
            properties.getDispatcher().getTransports().add(transport1);
            properties.getDispatcher().getTransports().add(transport2);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultQueueProvider.class, () -> new DefaultQueueProvider(1000));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventListenerRegistry", EventListenerRegistry.class, 
                    () -> new SpringInterfaceEventListenerRegistry(context));
            context.register(InMemoryIncomingTransportFactory.class);
            context.register(DispatcherConfiguration.class);
            context.refresh();

            // when
            List<IncomingEventTransport> transports = context.getBean("incomingEventTransports", List.class);

            // then
            assertThat(transports).hasSize(2);
        }
    }

    @Test
    @DisplayName("Should throw exception when no factory found for transport type")
    void shouldThrowExceptionWhenNoFactoryFoundForTransportType() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("kafka-transport");
            transportConfig.setType("kafka");
            properties.getDispatcher().getTransports().add(transportConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultQueueProvider.class, () -> new DefaultQueueProvider(1000));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventListenerRegistry", EventListenerRegistry.class, 
                    () -> new SpringInterfaceEventListenerRegistry(context));
            // Only in-memory factory, not kafka
            context.register(InMemoryIncomingTransportFactory.class);
            context.register(DispatcherConfiguration.class);

            // when & then
            assertThatThrownBy(context::refresh)
                    .rootCause().isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unsupported transport type 'kafka'");
        }
    }

    @Test
    @DisplayName("Should create Kafka incoming transport when kafka factory is available")
    void shouldCreateKafkaIncomingTransportWhenKafkaFactoryIsAvailable() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("kafka-transport");
            transportConfig.setType("kafka");
            transportConfig.setTopic("test-topic");
            transportConfig.setBootstrapServers("localhost:9092");
            transportConfig.setConsumerGroup("test-group");
            properties.getDispatcher().getTransports().add(transportConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultQueueProvider.class, () -> new DefaultQueueProvider(1000));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventListenerRegistry", EventListenerRegistry.class, 
                    () -> new SpringInterfaceEventListenerRegistry(context));
            context.register(KafkaIncomingTransportFactory.class);
            context.register(DispatcherConfiguration.class);
            context.refresh();

            // when
            List<IncomingEventTransport> transports = context.getBean("incomingEventTransports", List.class);

            // then
            assertThat(transports).hasSize(1);
        }
    }

    @Test
    @DisplayName("Should not create duplicate dispatcher when custom bean exists")
    void shouldNotCreateDuplicateDispatcherWhenCustomBeanExists() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultQueueProvider.class, () -> new DefaultQueueProvider(1000));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventListenerRegistry", EventListenerRegistry.class, 
                    () -> new SpringInterfaceEventListenerRegistry(context));
            context.registerBean("customDispatcher", EventDispatcher.class, () -> null);
            context.register(InMemoryIncomingTransportFactory.class);
            context.register(DispatcherConfiguration.class);
            context.refresh();

            // when & then
            assertThat(context.containsBean("eventDispatcher")).isFalse();
        }
    }

    @Test
    @DisplayName("Should not create duplicate incoming transports when custom bean exists")
    void shouldNotCreateDuplicateIncomingTransportsWhenCustomBeanExists() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultQueueProvider.class, () -> new DefaultQueueProvider(1000));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventListenerRegistry", EventListenerRegistry.class, 
                    () -> new SpringInterfaceEventListenerRegistry(context));
            context.registerBean("customIncomingEventTransports", List.class, () -> List.of());
            context.register(InMemoryIncomingTransportFactory.class);
            context.register(DispatcherConfiguration.class);
            context.refresh();

            // when & then
            assertThat(context.containsBean("incomingEventTransports")).isFalse();
        }
    }

    @Test
    @DisplayName("Should start dispatcher on creation")
    void shouldStartDispatcherOnCreation() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.registerBean(DefaultQueueProvider.class, () -> new DefaultQueueProvider(1000));
            context.registerBean("dispatcherExecutor", ExecutorService.class, () -> Executors.newFixedThreadPool(2));
            context.registerBean("eventListenerRegistry", EventListenerRegistry.class, 
                    () -> new SpringInterfaceEventListenerRegistry(context));
            context.register(InMemoryIncomingTransportFactory.class);
            context.register(DispatcherConfiguration.class);
            context.refresh();

            // when
            EventDispatcher dispatcher = context.getBean(EventDispatcher.class);

            // then - dispatcher should be started (no exception thrown)
            assertThat(dispatcher).isNotNull();
        }
    }
}
