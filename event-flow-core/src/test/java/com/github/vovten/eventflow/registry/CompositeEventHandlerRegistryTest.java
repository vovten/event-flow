package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.EventHandler;
import com.github.vovten.eventflow.EventSubscriber;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link CompositeEventHandlerRegistry}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("CompositeEventHandlerRegistry Tests")
class CompositeEventHandlerRegistryTest {

    private CompositeEventHandlerRegistry compositeRegistry;
    private EventSubscriberRegistry registry1;
    private EventSubscriberRegistry registry2;

    @BeforeEach
    void setUp() {
        registry1 = new EventSubscriberRegistry();
        registry2 = new EventSubscriberRegistry();
        compositeRegistry = new CompositeEventHandlerRegistry(List.of(registry1, registry2));
    }

    @Test
    @DisplayName("Should create with registries list")
    void shouldCreateWithRegistriesList() {
        // Act
        CompositeEventHandlerRegistry registry = new CompositeEventHandlerRegistry(List.of(registry1));

        // Assert
        assertNotNull(registry);
    }

    @Test
    @DisplayName("Should get handlers from all registries")
    void shouldGetHandlersFromAllRegistries() {
        // Arrange
        TestEventSubscriber subscriber1 = new TestEventSubscriber();
        TestEventSubscriber subscriber2 = new TestEventSubscriber();
        registry1.register(subscriber1);
        registry2.register(subscriber2);

        // Act
        List<EventHandler> handlers = compositeRegistry.getHandlers(new TestEvent("test"));

        // Assert
        assertEquals(2, handlers.size());
    }

    @Test
    @DisplayName("Should count handlers from all registries")
    void shouldCountHandlersFromAllRegistries() {
        // Arrange
        registry1.register(new TestEventSubscriber());
        registry2.register(new TestEventSubscriber());

        // Act
        int count = compositeRegistry.handlerCount();

        // Assert
        assertEquals(2, count);
    }

    @Test
    @DisplayName("Should register handler to all registries")
    void shouldRegisterHandlerToAllRegistries() {
        // Act
        compositeRegistry.register(new TestEventSubscriber());

        // Assert
        List<EventHandler> handlers = compositeRegistry.getHandlers(new TestEvent("test"));
        assertEquals(2, handlers.size());
    }

    @Test
    @DisplayName("Should unregister handler from all registries")
    void shouldUnregisterHandlerFromAllRegistries() {
        // Arrange
        TestEventSubscriber subscriber = new TestEventSubscriber();
        registry1.register(subscriber);
        registry2.register(subscriber);

        // Act
        boolean unregistered = compositeRegistry.unregister(subscriber);

        // Assert
        assertTrue(unregistered);
    }

    @Test
    @DisplayName("Should check if handler is registered in any registry")
    void shouldCheckIfHandlerIsRegisteredInAnyRegistry() {
        // Arrange
        TestEventSubscriber subscriber = new TestEventSubscriber();
        registry1.register(subscriber);

        // Act & Assert
        assertTrue(compositeRegistry.isRegistered(subscriber));
    }

    /**
     * Test event class.
     */
    private static final class TestEvent extends AbstractTraceableEvent {
        private final String data;

        TestEvent(String data) {
            super();
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @Override
        public String asJson() {
            return "{\"data\":\"" + data + "\"}";
        }
    }

    /**
     * Test subscriber class.
     */
    private static final class TestEventSubscriber implements EventSubscriber {
        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }
}
