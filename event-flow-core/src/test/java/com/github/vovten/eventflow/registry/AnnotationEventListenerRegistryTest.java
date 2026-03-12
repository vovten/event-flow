package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.annotation.EventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link AnnotationEventListenerRegistry}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("AnnotationEventListenerRegistry Tests")
class AnnotationEventListenerRegistryTest {

    private AnnotationEventListenerRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new AnnotationEventListenerRegistry();
    }

    @Test
    @DisplayName("Should register listener with annotated method")
    void shouldRegisterListenerWithAnnotatedMethod() {
        // Arrange
        AnnotatedListener listener = new AnnotatedListener();

        // Act
        registry.register(listener);

        // Assert
        var listeners = registry.getListeners(new TestEvent("test"));
        assertEquals(1, listeners.size());
    }

    @Test
    @DisplayName("Should register listener with multiple annotated methods")
    void shouldRegisterListenerWithMultipleAnnotatedMethods() {
        // Arrange
        MultiMethodListener listener = new MultiMethodListener();

        // Act
        registry.register(listener);

        // Assert
        assertEquals(2, registry.listenerCount());
    }

    @Test
    @DisplayName("Should register generic listener for Event.class")
    void shouldRegisterGenericListenerForEventClass() {
        // Arrange
        GenericListener listener = new GenericListener();

        // Act
        registry.register(listener);

        // Assert
        var listeners = registry.getListeners(new TestEvent("test"));
        assertEquals(1, listeners.size());
    }

    @Test
    @DisplayName("Should throw exception for invalid method signature")
    void shouldThrowExceptionForInvalidMethodSignature() {
        // Arrange
        InvalidListener listener = new InvalidListener();

        // Assert
        assertThrows(InvalidEventListenerMethodSignatureException.class, () ->
                registry.register(listener)
        );
    }

    @Test
    @DisplayName("Should unregister listener")
    void shouldUnregisterListener() {
        // Arrange
        AnnotatedListener listener = new AnnotatedListener();
        registry.register(listener);

        // Act
        boolean unregistered = registry.unregister(listener);

        // Assert
        assertTrue(unregistered);
        assertTrue(registry.getListeners(new TestEvent("test")).isEmpty());
    }

    @Test
    @DisplayName("Should return false when unregistering non-existent listener")
    void shouldReturnFalseWhenUnregisteringNonExistentListener() {
        // Arrange
        AnnotatedListener listener = new AnnotatedListener();

        // Act
        boolean unregistered = registry.unregister(listener);

        // Assert
        assertFalse(unregistered);
    }

    @Test
    @DisplayName("Should check if listener is registered")
    void shouldCheckIfListenerIsRegistered() {
        // Arrange
        AnnotatedListener listener = new AnnotatedListener();
        registry.register(listener);

        // Act & Assert
        assertTrue(registry.isRegistered(listener));
        assertFalse(registry.isRegistered(new Object()));
    }

    @Test
    @DisplayName("Should throw exception for merge operation")
    void shouldThrowExceptionForMergeOperation() {
        // Arrange
        InterfaceEventListenerRegistry otherRegistry = new InterfaceEventListenerRegistry();

        // Assert
        assertThrows(UnsupportedOperationException.class, () ->
                registry.merge(otherRegistry)
        );
    }

    @Test
    @DisplayName("Should not register duplicate listener")
    void shouldNotRegisterDuplicateListener() {
        // Arrange
        AnnotatedListener listener = new AnnotatedListener();

        // Act
        registry.register(listener);
        registry.register(listener);

        // Assert
        var listeners = registry.getListeners(new TestEvent("test"));
        assertEquals(1, listeners.size());
    }

    /**
     * Test event class.
     */
    private static final class TestEvent implements Event {
        private final String data;

        TestEvent(String data) {
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
     * Another test event class.
     */
    private static final class AnotherEvent implements Event {
        private final String data;

        AnotherEvent(String data) {
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return AnotherEvent.class;
        }

        @Override
        public String asJson() {
            return "{\"data\":\"" + data + "\"}";
        }
    }

    /**
     * Listener with annotated method.
     */
    private static final class AnnotatedListener {
        @EventListener
        public void handleTestEvent(TestEvent event) {
        }
    }

    /**
     * Listener with multiple annotated methods.
     */
    private static final class MultiMethodListener {
        @EventListener
        public void handleTestEvent(TestEvent event) {
        }

        @EventListener
        public void handleAnotherEvent(AnotherEvent event) {
        }
    }

    /**
     * Generic listener for all events.
     */
    private static final class GenericListener {
        @EventListener
        public void handleEvent(Event event) {
        }
    }

    /**
     * Listener with invalid method signature.
     */
    private static final class InvalidListener {
        @EventListener
        public void handleEvent(String invalidParam) {
        }
    }
}
