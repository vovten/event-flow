package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.annotation.EventListener;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for EventListenerRegistryBuilder
 */
class EventListenerRegistryBuilderIntegrationTest {

    @Test
    @DisplayName("Should build registry with Spring context")
    void shouldBuildRegistryWithSpringContext() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.register(TestConfiguration.class);
            context.refresh();

            EventListenerRegistry registry = EventListenerRegistryBuilder.spring()
                    .scanPackage("com.github.vovten.eventflow.registry")
                    .withSpringContext(context)
                    .build();

            assertThat(registry).isNotNull();
            assertThat(registry.listenerCount()).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    @DisplayName("Should discover real listeners from Spring context")
    void shouldDiscoverRealListenersFromSpringContext() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.register(RealListener.class);
            context.refresh();

            EventListenerRegistry registry = EventListenerRegistryBuilder.spring()
                    .scanPackage("com.github.vovten.eventflow.registry")
                    .withSpringContext(context)
                    .build();

            TestEvent event = new TestEvent();
            assertThat(registry.getListeners(event)).isNotEmpty();
        }
    }

    @Test
    @DisplayName("Should create composite registry with multiple registries")
    void shouldCreateCompositeRegistryWithMultipleRegistries() {
        EventListenerRegistry annRegistry = new AnnotationEventListenerRegistry();
        EventListenerRegistry intRegistry = new InterfaceEventListenerRegistry();

        EventListenerRegistry composite = EventListenerRegistryBuilder.composite()
                .withRegistry(annRegistry)
                .withRegistry(intRegistry)
                .build();

        assertThat(composite).isInstanceOf(CompositeEventListenerRegistry.class);
    }

    @Component
    static class RealListener {
        @EventListener
        public void handle(TestEvent event) {
            // real implementation
        }
    }

    @Configuration
    static class TestConfiguration {
        // empty config
    }
}