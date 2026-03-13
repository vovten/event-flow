package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.EventSubscriber;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventHandlerRegistryBuilder.
 */
@DisplayName("EventHandlerRegistryBuilder Tests")
class EventHandlerRegistryBuilderTest {

    @Test
    @DisplayName("Should throw exception when building without handlers")
    void shouldThrowExceptionWhenBuildingWithoutHandlers() {
        assertThrows(IllegalStateException.class, () ->
                EventHandlerRegistryBuilder.create()
                        .build());
    }

    @Test
    @DisplayName("Should build annotation-based registry without Spring")
    void shouldBuildAnnotationBasedRegistryWithoutSpring() {
        EventHandlerRegistry registry = EventHandlerRegistryBuilder.create()
                .withAnnotationListeners()
                .build();

        assertNotNull(registry);
        assertInstanceOf(EventListenerRegistry.class, registry);
    }

    @Test
    @DisplayName("Should build interface-based registry without Spring")
    void shouldBuildInterfaceBasedRegistryWithoutSpring() {
        EventHandlerRegistry registry = EventHandlerRegistryBuilder.create()
                .withInterfaceListeners()
                .build();

        assertNotNull(registry);
        assertInstanceOf(EventSubscriberRegistry.class, registry);
    }

    @Test
    @DisplayName("Should build composite registry with multiple handler types")
    void shouldBuildCompositeRegistryWithMultipleHandlerTypes() {
        EventHandlerRegistry registry = EventHandlerRegistryBuilder.create()
                .withAnnotationListeners()
                .withInterfaceListeners()
                .build();

        assertNotNull(registry);
        assertInstanceOf(CompositeEventHandlerRegistry.class, registry);
    }

    @Test
    @DisplayName("Should add custom registry")
    void shouldAddCustomRegistry() {
        EventListenerRegistry customRegistry = new EventListenerRegistry();

        EventHandlerRegistry registry = EventHandlerRegistryBuilder.create()
                .withCustomRegistry(customRegistry)
                .build();

        assertNotNull(registry);
        assertEquals(customRegistry, registry);
    }

    @Test
    @DisplayName("Should ignore null custom registry")
    void shouldIgnoreNullCustomRegistry() {
        assertThrows(IllegalStateException.class, () ->
                EventHandlerRegistryBuilder.create()
                        .withCustomRegistry(null)
                        .build());
    }

    @Test
    @DisplayName("Should apply decorator to registry")
    void shouldApplyDecoratorToRegistry() {
        EventHandlerRegistry registry = EventHandlerRegistryBuilder.create()
                .withAnnotationListeners()
                .withDecorator(r -> new CompositeEventHandlerRegistry(List.of(r)))
                .build();

        assertNotNull(registry);
        assertInstanceOf(CompositeEventHandlerRegistry.class, registry);
    }

    @Test
    @DisplayName("Should ignore null decorator")
    void shouldIgnoreNullDecorator() {
        EventHandlerRegistry registry = EventHandlerRegistryBuilder.create()
                .withAnnotationListeners()
                .withDecorator(null)
                .build();

        assertNotNull(registry);
        assertInstanceOf(EventListenerRegistry.class, registry);
    }

    @Test
    @DisplayName("Should build and log configuration")
    void shouldBuildAndLogConfiguration() {
        EventHandlerRegistry registry = EventHandlerRegistryBuilder.create()
                .withAnnotationListeners()
                .withInterfaceListeners()
                .buildAndLog();

        assertNotNull(registry);
        assertInstanceOf(CompositeEventHandlerRegistry.class, registry);
    }

    @Test
    @DisplayName("Should register annotation handler")
    void shouldRegisterAnnotationHandler() {
        EventListenerRegistry registry = (EventListenerRegistry) EventHandlerRegistryBuilder.create()
                .withAnnotationListeners()
                .build();

        TestAnnotatedHandler handler = new TestAnnotatedHandler();
        registry.register(handler);

        assertEquals(1, registry.handlerCount());
    }

    @Test
    @DisplayName("Should register interface subscriber")
    void shouldRegisterInterfaceSubscriber() {
        EventSubscriberRegistry registry = (EventSubscriberRegistry) EventHandlerRegistryBuilder.create()
                .withInterfaceListeners()
                .build();

        TestEventSubscriber subscriber = new TestEventSubscriber();
        registry.register(subscriber);

        assertEquals(1, registry.handlerCount());
    }

    static class TestAnnotatedHandler {
        @EventListener
        public void handleEvent(Event event) {
        }
    }

    static class TestEventSubscriber implements EventSubscriber {
        @Override
        public List<Class<? extends Event>> events() {
            return List.of(Event.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }
}
