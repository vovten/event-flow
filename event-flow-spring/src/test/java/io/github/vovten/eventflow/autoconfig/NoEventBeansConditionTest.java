package io.github.vovten.eventflow.autoconfig;

import io.github.vovten.eventflow.dispatcher.EventDispatcher;
import io.github.vovten.eventflow.dispatcher.HandlerResults;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.transport.AsyncEventConsumer;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Lazy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NoEventBeansCondition}.
 * @since 1.0.0
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

    @Test
    @DisplayName("DEFECT-7.1: Should not instantiate a lazy EventPublisher while evaluating the condition")
    void shouldNotInstantiateLazyPublisher() {
        LazyPublisherConfig.INSTANTIATIONS.set(0);

        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.register(LazyPublisherConfig.class, ConditionalBeanConfig.class);
            context.refresh();

            assertThat(context.containsBean("testConditionalBean"))
                    .as("A registered publisher definition must make the condition false")
                    .isFalse();
            assertThat(LazyPublisherConfig.INSTANTIATIONS)
                    .as("Condition evaluation must inspect bean definitions, not instantiate them")
                    .hasValue(0);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyPublisherConfig {

        static final AtomicInteger INSTANTIATIONS = new AtomicInteger();

        @Bean
        @Lazy
        public EventPublisher lazyEventPublisher() {
            INSTANTIATIONS.incrementAndGet();
            return event -> CompletableFuture.completedFuture(SendResults.empty());
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
        public CompletableFuture<HandlerResults> dispatch(Event event) {
            return CompletableFuture.completedFuture(HandlerResults.empty());
        }

        @Override
        public void register(Object listener) {
        }

        @Override
        public boolean isRegistered(Object listener) {
            return false;
        }

        @Override
        public void start(AsyncEventConsumer dispatchConsumer) {
        }

        @Override
        public void stop() {
        }
    }
}
