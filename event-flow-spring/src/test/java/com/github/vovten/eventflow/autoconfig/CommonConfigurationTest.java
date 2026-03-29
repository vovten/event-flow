package com.github.vovten.eventflow.autoconfig;

import com.github.vovten.eventflow.autoconfig.config.CommonConfiguration;
import com.github.vovten.eventflow.transport.DefaultLocalQueueProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link CommonConfiguration}.
 */
class CommonConfigurationTest {

    @Test
    @DisplayName("Should create executor service with custom properties")
    void shouldCreateExecutorServiceWithCustomProperties() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            properties.getDispatcher().getThreadPool().setCoreSize(2);
            properties.getDispatcher().getThreadPool().setMaxSize(8);
            properties.getDispatcher().getThreadPool().setQueueCapacity(50);
            properties.getDispatcher().getThreadPool().setKeepAliveSeconds(30);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.register(CommonConfiguration.class);
            context.refresh();

            // when
            ExecutorService executor = context.getBean("dispatcherExecutor", ExecutorService.class);

            // then
            assertThat(executor).isNotNull();
            assertThat(executor.isShutdown()).isFalse();
        }
    }

    @Test
    @DisplayName("Should create queue provider with default capacity")
    void shouldCreateQueueProviderWithDefaultCapacity() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            context.registerBean(EventFlowProperties.class, () -> properties);
            context.register(CommonConfiguration.class);
            context.refresh();

            // when
            DefaultLocalQueueProvider queueProvider = context.getBean(DefaultLocalQueueProvider.class);

            // then
            assertThat(queueProvider).isNotNull();
        }
    }

    @Test
    @DisplayName("Should create queue provider with custom capacity from local-queue transport")
    void shouldCreateQueueProviderWithCustomCapacityFromLocalQueueTransport() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            EventFlowProperties.TransportConfig transportConfig = new EventFlowProperties.TransportConfig();
            transportConfig.setName("local-queue");
            transportConfig.setCapacity(500);
            properties.getDispatcher().getTransports().add(transportConfig);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.register(CommonConfiguration.class);
            context.refresh();

            // when
            DefaultLocalQueueProvider queueProvider = context.getBean(DefaultLocalQueueProvider.class);

            // then
            assertThat(queueProvider).isNotNull();
        }
    }

    @Test
    @DisplayName("Should not create executor when event-flow is disabled")
    void shouldNotCreateExecutorWhenEventFlowIsDisabled() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            System.setProperty("event-flow.enabled", "false");
            EventFlowProperties properties = new EventFlowProperties();
            properties.setEnabled(false);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.register(CommonConfiguration.class);
            context.refresh();

            // when & then
            assertThat(context.containsBean("dispatcherExecutor")).isFalse();

            System.clearProperty("event-flow.enabled");
        }
    }

    @Test
    @DisplayName("Should still create queue provider when event-flow is disabled")
    void shouldStillCreateQueueProviderWhenEventFlowIsDisabled() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();
            properties.setEnabled(false);

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.register(CommonConfiguration.class);
            context.refresh();

            // when
            DefaultLocalQueueProvider queueProvider = context.getBean(DefaultLocalQueueProvider.class);

            // then
            assertThat(queueProvider).isNotNull();
        }
    }

    @Test
    @DisplayName("Should not create duplicate executor when custom bean exists")
    void shouldNotCreateDuplicateExecutorWhenCustomBeanExists() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.register(CustomExecutorConfig.class);
            context.refresh();

            // when
            ExecutorService executor = context.getBean("customExecutor", ExecutorService.class);

            // then
            assertThat(executor).isNotNull();
            assertThat(context.containsBean("dispatcherExecutor")).isFalse();
        }
    }

    @Test
    @DisplayName("Should not create duplicate queue provider when custom bean exists")
    void shouldNotCreateDuplicateQueueProviderWhenCustomBeanExists() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            EventFlowProperties properties = new EventFlowProperties();

            context.registerBean(EventFlowProperties.class, () -> properties);
            context.register(CustomQueueProviderConfig.class);
            context.register(CommonConfiguration.class);
            context.refresh();

            // when
            DefaultLocalQueueProvider queueProvider = context.getBean("customQueueProvider", DefaultLocalQueueProvider.class);

            // then
            assertThat(queueProvider).isNotNull();
            assertThat(context.containsBean("queueProvider")).isFalse();
        }
    }

    @Configuration
    static class CustomExecutorConfig {
        @Bean(name = "customExecutor")
        public ExecutorService customExecutor() {
            return java.util.concurrent.Executors.newFixedThreadPool(4);
        }
    }

    @Configuration
    static class CustomQueueProviderConfig {
        @Bean(name = "customQueueProvider")
        @ConditionalOnMissingBean
        public DefaultLocalQueueProvider customQueueProvider() {
            return new DefaultLocalQueueProvider(100);
        }
    }
}
