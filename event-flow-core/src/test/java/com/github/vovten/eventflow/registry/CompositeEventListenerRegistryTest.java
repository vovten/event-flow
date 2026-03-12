package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link CompositeEventListenerRegistry}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("CompositeEventListenerRegistry Tests")
class CompositeEventListenerRegistryTest {

    private CompositeEventListenerRegistry compositeRegistry;
    private InterfaceEventListenerRegistry registry1;
    private InterfaceEventListenerRegistry registry2;

    @BeforeEach
    void setUp() {
        registry1 = new InterfaceEventListenerRegistry();
        registry2 = new InterfaceEventListenerRegistry();
        compositeRegistry = new CompositeEventListenerRegistry(List.of(registry1, registry2));
    }

    @Test
    @DisplayName("Should create with registries list")
    void shouldCreateWithRegistriesList() {
        // Act
        CompositeEventListenerRegistry registry = new CompositeEventListenerRegistry(List.of(registry1));

        // Assert
        assertNotNull(registry);
    }

    @Test
    @DisplayName("Should get listeners from all registries")
    void shouldGetListenersFromAllRegistries() {
        // Arrange
        TestEventListener listener1 = new TestEventListener();
        TestEventListener listener2 = new TestEventListener();
        registry1.register(listener1);
        registry2.register(listener2);

        // Act
        List<EventListener> listeners = compositeRegistry.getListeners(new TestEvent("test"));

        // Assert
        assertEquals(2, listeners.size());
    }

    @Test
    @DisplayName("Should count listeners from all registries")
    void shouldCountListenersFromAllRegistries() {
        // Arrange
        registry1.register(new TestEventListener());
        registry2.register(new TestEventListener());

        // Act
        int count = compositeRegistry.listenerCount();

        // Assert
        assertEquals(2, count);
    }

    @Test
    @DisplayName("Should register listener to all registries")
    void shouldRegisterListenerToAllRegistries() {
        // Act
        compositeRegistry.register(new TestEventListener());

        // Assert
        List<EventListener> listeners = compositeRegistry.getListeners(new TestEvent("test"));
        assertEquals(2, listeners.size());
    }

    @Test
    @DisplayName("Should unregister listener from all registries")
    void shouldUnregisterListenerFromAllRegistries() {
        // Arrange
        TestEventListener listener = new TestEventListener();
        registry1.register(listener);
        registry2.register(listener);

        // Act
        boolean unregistered = compositeRegistry.unregister(listener);

        // Assert
        assertTrue(unregistered);
    }

    @Test
    @DisplayName("Should check if listener is registered in any registry")
    void shouldCheckIfListenerIsRegisteredInAnyRegistry() {
        // Arrange
        TestEventListener listener = new TestEventListener();
        registry1.register(listener);

        // Act & Assert
        assertTrue(compositeRegistry.isRegistered(listener));
    }

    @Test
    @DisplayName("Should throw exception when merging with unsupported registry")
    void shouldThrowExceptionWhenMergingWithUnsupportedRegistry() {
        // Arrange
        InterfaceEventListenerRegistry otherRegistry = new InterfaceEventListenerRegistry();

        // Assert
        assertThrows(UnsupportedOperationException.class, () ->
                compositeRegistry.merge(otherRegistry)
        );
    }

    /**
     * Test event class.
     */
    private static class TestEvent implements Event {
        private final String data;

        public TestEvent(String data) {
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
     * Test listener class.
     */
    private static class TestEventListener implements EventListener {
        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }
}
