package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.EventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link EventListenerRegistry}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("EventListenerRegistry Tests")
class EventListenerRegistryTest {

    private EventListenerRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new EventListenerRegistry();
    }

    @Test
    @DisplayName("Should register listener with annotated method")
    void shouldRegisterListenerWithAnnotatedMethod() {
        // Arrange
        AnnotatedListener listener = new AnnotatedListener();

        // Act
        registry.register(listener);

        // Assert
        var handlers = registry.getHandlers(new TestEvent("test"));
        assertEquals(1, handlers.size());
    }

    @Test
    @DisplayName("Should register listener with multiple annotated methods")
    void shouldRegisterListenerWithMultipleAnnotatedMethods() {
        // Arrange
        MultiMethodListener listener = new MultiMethodListener();

        // Act
        registry.register(listener);

        // Assert
        assertEquals(2, registry.handlerCount());
    }

    @Test
    @DisplayName("Should register generic listener for Event.class")
    void shouldRegisterGenericListenerForEventClass() {
        // Arrange
        GenericListener listener = new GenericListener();

        // Act
        registry.register(listener);

        // Assert
        var handlers = registry.getHandlers(new TestEvent("test"));
        assertEquals(1, handlers.size());
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
        assertTrue(registry.getHandlers(new TestEvent("test")).isEmpty());
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
        EventSubscriberRegistry otherRegistry = new EventSubscriberRegistry();

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
        var handlers = registry.getHandlers(new TestEvent("test"));
        assertEquals(1, handlers.size());
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
     * Another test event class.
     */
    private static final class AnotherEvent extends AbstractTraceableEvent {
        private final String data;

        AnotherEvent(String data) {
            super();
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
