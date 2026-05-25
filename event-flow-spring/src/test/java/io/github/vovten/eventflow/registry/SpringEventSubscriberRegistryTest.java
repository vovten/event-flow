package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.EventSubscriber;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for SpringEventSubscriberRegistry.
 * @since 1.0.0
 */
@DisplayName("SpringEventSubscriberRegistry Tests")
class SpringEventSubscriberRegistryTest {

    private ApplicationContext applicationContext;
    private SpringEventSubscriberRegistry registry;

    @BeforeEach
    void setUp() {
        applicationContext = mock(ApplicationContext.class);
        when(applicationContext.getBeansOfType(EventSubscriber.class)).thenReturn(Map.of());
        registry = new SpringEventSubscriberRegistry(applicationContext);
    }

    @Test
    @DisplayName("Should throw exception for null context")
    void shouldThrowExceptionForNullContext() {
        assertThrows(IllegalArgumentException.class, () ->
                new SpringEventSubscriberRegistry(null));
    }

    @Test
    @DisplayName("Should create registry with valid context")
    void shouldCreateRegistryWithValidContext() {
        assertDoesNotThrow(() ->
                new SpringEventSubscriberRegistry(applicationContext));
    }

    @Test
    @DisplayName("Should register interface subscriber")
    void shouldRegisterInterfaceSubscriber() {
        TestEventSubscriber subscriber = new TestEventSubscriber();
        registry.register(subscriber);

        assertEquals(1, registry.handlerCount());
        assertTrue(registry.isRegistered(subscriber));
    }

    @Test
    @DisplayName("Should ignore non-interface subscriber")
    void shouldIgnoreNonInterfaceSubscriber() {
        Object nonSubscriber = new Object();
        registry.register(nonSubscriber);

        assertEquals(0, registry.handlerCount());
        assertFalse(registry.isRegistered(nonSubscriber));
    }

    @Test
    @DisplayName("Should return handlers for event type")
    void shouldReturnHandlersForEventType() {
        TestEventSubscriber subscriber = new TestEventSubscriber();
        registry.register(subscriber);

        TestEvent event = new TestEvent();
        List<EventHandler> handlers = registry.getHandlers(event);

        assertEquals(1, handlers.size());
        assertTrue(handlers.contains(subscriber));
    }

    @Test
    @DisplayName("Should include generic subscribers")
    void shouldIncludeGenericSubscribers() {
        SpecificEventSubscriber specificSubscriber = new SpecificEventSubscriber();
        GenericEventSubscriber genericSubscriber = new GenericEventSubscriber();

        registry.register(specificSubscriber);
        registry.register(genericSubscriber);

        SpecificEvent event = new SpecificEvent();
        List<EventHandler> handlers = registry.getHandlers(event);

        assertEquals(2, handlers.size());
        assertTrue(handlers.contains(specificSubscriber));
        assertTrue(handlers.contains(genericSubscriber));
    }

    @Test
    @DisplayName("Should return empty list when no subscribers")
    void shouldReturnEmptyListWhenNoSubscribers() {
        TestEvent event = new TestEvent();
        List<EventHandler> handlers = registry.getHandlers(event);

        assertTrue(handlers.isEmpty());
    }

    @Test
    @DisplayName("Should unregister existing subscriber")
    void shouldUnregisterExistingSubscriber() {
        TestEventSubscriber subscriber = new TestEventSubscriber();
        registry.register(subscriber);

        boolean result = registry.unregister(subscriber);

        assertTrue(result);
        assertFalse(registry.isRegistered(subscriber));
    }

    @Test
    @DisplayName("Should return false when unregistering non-interface subscriber")
    void shouldReturnFalseWhenUnregisteringNonInterfaceSubscriber() {
        Object nonSubscriber = new Object();
        boolean result = registry.unregister(nonSubscriber);

        assertFalse(result);
    }

    @Test
    @DisplayName("Should return true for registered interface subscriber")
    void shouldReturnTrueForRegisteredInterfaceSubscriber() {
        TestEventSubscriber subscriber = new TestEventSubscriber();
        registry.register(subscriber);

        assertTrue(registry.isRegistered(subscriber));
    }

    @Test
    @DisplayName("Should return false for non-interface subscriber")
    void shouldReturnFalseForNonInterfaceSubscriber() {
        Object nonSubscriber = new Object();
        assertFalse(registry.isRegistered(nonSubscriber));
    }

    @Test
    @DisplayName("Should throw exception on merge")
    void shouldThrowExceptionOnMerge() {
        SpringEventSubscriberRegistry otherRegistry = mock(SpringEventSubscriberRegistry.class);

        assertThrows(UnsupportedOperationException.class, () -> registry.merge(otherRegistry));
    }

    @Test
    @DisplayName("Should register subscriber for multiple event types")
    void shouldRegisterSubscriberForMultipleEventTypes() {
        MultiEventSubscriber subscriber = new MultiEventSubscriber();
        registry.register(subscriber);

        assertEquals(2, registry.handlerCount());

        List<EventHandler> handlers1 = registry.getHandlers(new TestEvent());
        List<EventHandler> handlers2 = registry.getHandlers(new SpecificEvent());

        assertEquals(1, handlers1.size());
        assertEquals(1, handlers2.size());
        assertTrue(handlers1.contains(subscriber));
        assertTrue(handlers2.contains(subscriber));
    }

    static class TestEventSubscriber implements EventSubscriber {
        @Override
        public List<Class<?>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    static class TestEvent extends AbstractTraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }

    static class SpecificEventSubscriber implements EventSubscriber {
        @Override
        public List<Class<?>> events() {
            return List.of(SpecificEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    static class SpecificEvent extends AbstractTraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return SpecificEvent.class;
        }
    }

    static class GenericEventSubscriber implements EventSubscriber {
        @Override
        public List<Class<?>> events() {
            return List.of(Event.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    static class MultiEventSubscriber implements EventSubscriber {
        @Override
        public List<Class<?>> events() {
            return List.of(TestEvent.class, SpecificEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }
}
