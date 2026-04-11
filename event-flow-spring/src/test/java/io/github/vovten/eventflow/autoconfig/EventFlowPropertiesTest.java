package io.github.vovten.eventflow.autoconfig;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link EventFlowProperties}.
 */
class EventFlowPropertiesTest {

    @Test
    @DisplayName("Should load default properties correctly")
    void shouldLoadDefaultPropertiesCorrectly() {
        // given
        Map<String, String> properties = Map.of();
        ConfigurationPropertySource source = new MapConfigurationPropertySource(properties);
        Binder binder = new Binder(source);

        // when
        EventFlowProperties eventFlowProperties = binder.bind("event-flow", EventFlowProperties.class)
                .orElseGet(EventFlowProperties::new);

        // then
        assertThat(eventFlowProperties.isEnabled()).isFalse();
        assertThat(eventFlowProperties.getDispatcher().getListenerPackages()).isEmpty();
        assertThat(eventFlowProperties.getPublisher().isEnabled()).isFalse();
        assertThat(eventFlowProperties.getPublisher().isTransactional()).isTrue();
        assertThat(eventFlowProperties.getPublisher().getRetry().isEnabled()).isFalse();
        assertThat(eventFlowProperties.getPublisher().getRetry().getMaxAttempts()).isEqualTo(3);
        assertThat(eventFlowProperties.getPublisher().getRetry().getInitialDelay()).isEqualTo(Duration.ofMillis(100));
        assertThat(eventFlowProperties.getPublisher().getRetry().getMultiplier()).isEqualTo(2.0);
        assertThat(eventFlowProperties.getPublisher().getChannels()).isEmpty();
        assertThat(eventFlowProperties.getDispatcher().isEnabled()).isFalse();
        assertThat(eventFlowProperties.getDispatcher().getThreadPool().getCoreSize()).isEqualTo(4);
        assertThat(eventFlowProperties.getDispatcher().getThreadPool().getMaxSize()).isEqualTo(16);
        assertThat(eventFlowProperties.getDispatcher().getThreadPool().getQueueCapacity()).isEqualTo(100);
        assertThat(eventFlowProperties.getDispatcher().getThreadPool().getKeepAliveSeconds()).isEqualTo(60);
        assertThat(eventFlowProperties.getDispatcher().getTransports()).isEmpty();
    }

    @Test
    @DisplayName("Should bind custom publisher configuration")
    void shouldBindCustomPublisherConfiguration() {
        // given
        Map<String, String> properties = Map.of(
                "event-flow.publisher.enabled", "false",
                "event-flow.publisher.transactional", "false",
                "event-flow.publisher.retry.enabled", "true",
                "event-flow.publisher.retry.max-attempts", "5",
                "event-flow.publisher.retry.initial-delay", "200ms",
                "event-flow.publisher.retry.multiplier", "3.0"
        );
        ConfigurationPropertySource source = new MapConfigurationPropertySource(properties);
        Binder binder = new Binder(source);

        // when
        EventFlowProperties eventFlowProperties = binder.bind("event-flow", EventFlowProperties.class)
                .orElseGet(EventFlowProperties::new);

        // then
        assertThat(eventFlowProperties.getPublisher().isEnabled()).isFalse();
        assertThat(eventFlowProperties.getPublisher().isTransactional()).isFalse();
        assertThat(eventFlowProperties.getPublisher().getRetry().isEnabled()).isTrue();
        assertThat(eventFlowProperties.getPublisher().getRetry().getMaxAttempts()).isEqualTo(5);
        assertThat(eventFlowProperties.getPublisher().getRetry().getInitialDelay()).isEqualTo(Duration.ofMillis(200));
        assertThat(eventFlowProperties.getPublisher().getRetry().getMultiplier()).isEqualTo(3.0);
    }

    @Test
    @DisplayName("Should bind custom dispatcher thread pool configuration")
    void shouldBindCustomDispatcherThreadPoolConfiguration() {
        // given
        Map<String, String> properties = Map.of(
                "event-flow.dispatcher.enabled", "false",
                "event-flow.dispatcher.thread-pool.core-size", "8",
                "event-flow.dispatcher.thread-pool.max-size", "32",
                "event-flow.dispatcher.thread-pool.queue-capacity", "200",
                "event-flow.dispatcher.thread-pool.keep-alive-seconds", "120"
        );
        ConfigurationPropertySource source = new MapConfigurationPropertySource(properties);
        Binder binder = new Binder(source);

        // when
        EventFlowProperties eventFlowProperties = binder.bind("event-flow", EventFlowProperties.class)
                .orElseGet(EventFlowProperties::new);

        // then
        assertThat(eventFlowProperties.getDispatcher().isEnabled()).isFalse();
        assertThat(eventFlowProperties.getDispatcher().getThreadPool().getCoreSize()).isEqualTo(8);
        assertThat(eventFlowProperties.getDispatcher().getThreadPool().getMaxSize()).isEqualTo(32);
        assertThat(eventFlowProperties.getDispatcher().getThreadPool().getQueueCapacity()).isEqualTo(200);
        assertThat(eventFlowProperties.getDispatcher().getThreadPool().getKeepAliveSeconds()).isEqualTo(120);
    }

    @Test
    @DisplayName("Should bind custom channel configuration")
    void shouldBindCustomChannelConfiguration() {
        // given
        Map<String, String> properties = Map.of(
                "event-flow.publisher.channels[0].name", "internal",
                "event-flow.publisher.channels[0].transports[0].name", "local-queue",
                "event-flow.publisher.channels[0].transports[0].capacity", "500",
                "event-flow.publisher.channels[1].name", "external",
                "event-flow.publisher.channels[1].transports[0].name", "kafka",
                "event-flow.publisher.channels[1].transports[0].topic", "events-topic",
                "event-flow.publisher.channels[1].transports[0].servers", "localhost:9092"
        );
        ConfigurationPropertySource source = new MapConfigurationPropertySource(properties);
        Binder binder = new Binder(source);

        // when
        EventFlowProperties eventFlowProperties = binder.bind("event-flow", EventFlowProperties.class)
                .orElseGet(EventFlowProperties::new);

        // then
        assertThat(eventFlowProperties.getPublisher().getChannels()).hasSize(2);

        var internalChannel = eventFlowProperties.getPublisher().getChannels().get(0);
        assertThat(internalChannel.getName()).isEqualTo("internal");
        assertThat(internalChannel.getTransports()).hasSize(1);
        assertThat(internalChannel.getTransports().get(0).getName()).isEqualTo("local-queue");
        assertThat(internalChannel.getTransports().get(0).getCapacity()).isEqualTo(500);

        var externalChannel = eventFlowProperties.getPublisher().getChannels().get(1);
        assertThat(externalChannel.getName()).isEqualTo("external");
        assertThat(externalChannel.getTransports()).hasSize(1);
        var kafkaTransport = externalChannel.getTransports().get(0);
        assertThat(kafkaTransport.getName()).isEqualTo("kafka");
        assertThat(kafkaTransport.getTopic()).isEqualTo("events-topic");
        assertThat(kafkaTransport.getServers()).isEqualTo("localhost:9092");
    }

    @Test
    @DisplayName("Should bind custom dispatcher transports configuration")
    void shouldBindCustomDispatcherTransportsConfiguration() {
        // given
        Map<String, String> properties = Map.of(
                "event-flow.dispatcher.transports[0].name", "local-queue",
                "event-flow.dispatcher.transports[0].capacity", "500",
                "event-flow.dispatcher.transports[1].name", "kafka",
                "event-flow.dispatcher.transports[1].topic", "events-topic",
                "event-flow.dispatcher.transports[1].servers", "localhost:9092",
                "event-flow.dispatcher.transports[1].consumerGroup", "custom-group"
        );
        ConfigurationPropertySource source = new MapConfigurationPropertySource(properties);
        Binder binder = new Binder(source);

        // when
        EventFlowProperties eventFlowProperties = binder.bind("event-flow", EventFlowProperties.class)
                .orElseGet(EventFlowProperties::new);

        // then
        assertThat(eventFlowProperties.getDispatcher().getTransports()).hasSize(2);

        var localQueueTransport = eventFlowProperties.getDispatcher().getTransports().get(0);
        assertThat(localQueueTransport.getName()).isEqualTo("local-queue");
        assertThat(localQueueTransport.getCapacity()).isEqualTo(500);

        var kafkaTransport = eventFlowProperties.getDispatcher().getTransports().get(1);
        assertThat(kafkaTransport.getName()).isEqualTo("kafka");
        assertThat(kafkaTransport.getTopic()).isEqualTo("events-topic");
        assertThat(kafkaTransport.getServers()).isEqualTo("localhost:9092");
        assertThat(kafkaTransport.getConsumerGroup()).isEqualTo("custom-group");
    }

    @Test
    @DisplayName("Should bind listener-packages property")
    void shouldBindListenerPackagesProperty() {
        // given
        Map<String, String> properties = Map.of(
                "event-flow.dispatcher.listener-packages", "com.example.listener"
        );
        ConfigurationPropertySource source = new MapConfigurationPropertySource(properties);
        Binder binder = new Binder(source);

        // when
        EventFlowProperties eventFlowProperties = binder.bind("event-flow", EventFlowProperties.class)
                .orElseGet(EventFlowProperties::new);

        // then
        assertThat(eventFlowProperties.getDispatcher().getListenerPackages()).isEqualTo("com.example.listener");
    }

    @Test
    @DisplayName("Should bind enabled property to false")
    void shouldBindEnabledPropertyToFalse() {
        // given
        Map<String, String> properties = Map.of(
                "event-flow.enabled", "false"
        );
        ConfigurationPropertySource source = new MapConfigurationPropertySource(properties);
        Binder binder = new Binder(source);

        // when
        EventFlowProperties eventFlowProperties = binder.bind("event-flow", EventFlowProperties.class)
                .orElseGet(EventFlowProperties::new);

        // then
        assertThat(eventFlowProperties.isEnabled()).isFalse();
    }

    @Test
    @DisplayName("Should bind complex configuration with all features")
    void shouldBindComplexConfigurationWithAllFeatures() {
        // given
        Map<String, String> properties = Map.ofEntries(
                Map.entry("event-flow.enabled", "true"),
                Map.entry("event-flow.dispatcher.listener-packages", "com.example.listener"),
                Map.entry("event-flow.publisher.enabled", "true"),
                Map.entry("event-flow.publisher.transactional", "true"),
                Map.entry("event-flow.publisher.silent", "false"),
                Map.entry("event-flow.publisher.retry.enabled", "true"),
                Map.entry("event-flow.publisher.retry.max-attempts", "3"),
                Map.entry("event-flow.publisher.channels[0].name", "internal"),
                Map.entry("event-flow.publisher.channels[0].transports[0].name", "local-queue"),
                Map.entry("event-flow.dispatcher.enabled", "true"),
                Map.entry("event-flow.dispatcher.thread-pool.core-size", "4"),
                Map.entry("event-flow.dispatcher.thread-pool.max-size", "16"),
                Map.entry("event-flow.dispatcher.transports[0].name", "local-queue"),
                Map.entry("event-flow.dispatcher.transports[0].capacity", "1000")
        );
        ConfigurationPropertySource source = new MapConfigurationPropertySource(properties);
        Binder binder = new Binder(source);

        // when
        EventFlowProperties eventFlowProperties = binder.bind("event-flow", EventFlowProperties.class)
                .orElseGet(EventFlowProperties::new);

        // then
        assertThat(eventFlowProperties.isEnabled()).isTrue();
        assertThat(eventFlowProperties.getDispatcher().getListenerPackages()).isEqualTo("com.example.listener");
        assertThat(eventFlowProperties.getPublisher().isEnabled()).isTrue();
        assertThat(eventFlowProperties.getPublisher().isTransactional()).isTrue();
        assertThat(eventFlowProperties.getPublisher().getRetry().isEnabled()).isTrue();
        assertThat(eventFlowProperties.getPublisher().getChannels()).hasSize(1);
        assertThat(eventFlowProperties.getDispatcher().isEnabled()).isTrue();
        assertThat(eventFlowProperties.getDispatcher().getThreadPool().getCoreSize()).isEqualTo(4);
        assertThat(eventFlowProperties.getDispatcher().getTransports()).hasSize(1);
    }
}
