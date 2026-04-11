package io.github.vovten.eventflow.autoconfig;

import io.github.vovten.eventflow.dispatcher.EventDispatcher;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Conditional;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NoEventBeansCondition}.
 */
class NoEventBeansConditionTest {

    @Test
    @DisplayName("Should not match when EventPublisher bean exists")
    void shouldNotMatchWhenEventPublisherBeanExists() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.register(PublisherConfig.class, ConditionalBeanConfig.class);
            context.refresh();

            // when & then
            assertThat(context.containsBean("testConditionalBean")).isFalse();
        }
    }

    @Test
    @DisplayName("Should not match when EventDispatcher bean exists")
    void shouldNotMatchWhenEventDispatcherBeanExists() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.register(DispatcherConfig.class, ConditionalBeanConfig.class);
            context.refresh();

            // when & then
            assertThat(context.containsBean("testConditionalBean")).isFalse();
        }
    }

    @Test
    @DisplayName("Should not match when both EventPublisher and EventDispatcher beans exist")
    void shouldNotMatchWhenBothBeansExist() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.register(BothConfig.class, ConditionalBeanConfig.class);
            context.refresh();

            // when & then
            assertThat(context.containsBean("testConditionalBean")).isFalse();
        }
    }

    @Test
    @DisplayName("Should match when neither EventPublisher nor EventDispatcher beans exist")
    void shouldMatchWhenNoBeansExist() {
        // given
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.register(EmptyConfig.class, ConditionalBeanConfig.class);
            context.refresh();

            // when & then
            assertThat(context.containsBean("testConditionalBean")).isTrue();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ConditionalBeanConfig {
        @Bean
        @Conditional(NoEventBeansCondition.class)
        public TestConditionalBean testConditionalBean() {
            return new TestConditionalBean();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PublisherConfig {
        @Bean
        public EventPublisher eventPublisher() {
            return event -> CompletableFuture.completedFuture(SendResults.empty());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class DispatcherConfig {
        @Bean
        public EventDispatcher eventDispatcher() {
            return new TestEventDispatcher();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class BothConfig {
        @Bean
        public EventPublisher eventPublisher() {
            return event -> CompletableFuture.completedFuture(SendResults.empty());
        }

        @Bean
        public EventDispatcher eventDispatcher() {
            return new TestEventDispatcher();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class EmptyConfig {
        // No beans registered
    }

    static class TestConditionalBean {
    }

    static class TestEventDispatcher implements EventDispatcher {

        @Override
        public void dispatch(Event event) {
        }

        @Override
        public void register(Object listener) {
        }

        @Override
        public boolean isRegistered(Object listener) {
            return false;
        }

        @Override
        public void start(Consumer<Event> dispatchConsumer) {
        }

        @Override
        public void stop() {
        }
    }
}
