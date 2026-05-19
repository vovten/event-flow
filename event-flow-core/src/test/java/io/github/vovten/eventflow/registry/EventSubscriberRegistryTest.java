package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.EventSubscriber;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link EventSubscriberRegistry}.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
@DisplayName("EventSubscriberRegistry Tests")
class EventSubscriberRegistryTest {

    private EventSubscriberRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new EventSubscriberRegistry();
    }

    @Test
    @DisplayName("Should register subscriber for specific event type")
    void shouldRegisterSubscriberForSpecificEventType() {
        // Arrange
        TestEventSubscriber subscriber = new TestEventSubscriber();

        // Act
        registry.register(subscriber);

        // Assert
        List<EventHandler> handlers = registry.getHandlers(new TestEvent("test"));
        assertEquals(1, handlers.size());
        assertTrue(handlers.contains(subscriber));
    }

    @Test
    @DisplayName("Should register subscriber for multiple event types")
    void shouldRegisterSubscriberForMultipleEventTypes() {
        // Arrange
        MultiEventSubscriber subscriber = new MultiEventSubscriber();

        // Act
        registry.register(subscriber);

        // Assert
        List<EventHandler> testHandlers = registry.getHandlers(new TestEvent("test"));
        List<EventHandler> anotherHandlers = registry.getHandlers(new AnotherEvent("test"));
        assertTrue(testHandlers.contains(subscriber));
        assertTrue(anotherHandlers.contains(subscriber));
    }

    @Test
    @DisplayName("Should register generic subscriber for all events")
    void shouldRegisterGenericSubscriberForAllEvents() {
        // Arrange
        GenericEventSubscriber genericSubscriber = new GenericEventSubscriber();

        // Act
        registry.register(genericSubscriber);

        // Assert
        List<EventHandler> handlers = registry.getHandlers(new TestEvent("test"));
        assertEquals(1, handlers.size());
    }

    @Test
    @DisplayName("Should return empty list when no subscribers")
    void shouldReturnEmptyListWhenNoSubscribers() {
        // Act
        List<EventHandler> handlers = registry.getHandlers(new TestEvent("test"));

        // Assert
        assertTrue(handlers.isEmpty());
    }

    @Test
    @DisplayName("Should count unique event types")
    void shouldCountUniqueEventTypes() {
        // Arrange
        registry.register(new TestEventSubscriber());
        registry.register(new AnotherEventSubscriber());

        // Act
        int count = registry.handlerCount();

        // Assert
        assertEquals(2, count);
    }

    @Test
    @DisplayName("Should unregister subscriber")
    void shouldUnregisterSubscriber() {
        // Arrange
        TestEventSubscriber subscriber = new TestEventSubscriber();
        registry.register(subscriber);

        // Act
        boolean unregistered = registry.unregister(subscriber);

        // Assert
        assertTrue(unregistered);
        assertTrue(registry.getHandlers(new TestEvent("test")).isEmpty());
    }

    @Test
    @DisplayName("Should return false when unregistering non-existent subscriber")
    void shouldReturnFalseWhenUnregisteringNonExistentSubscriber() {
        // Arrange
        TestEventSubscriber subscriber = new TestEventSubscriber();

        // Act
        boolean unregistered = registry.unregister(subscriber);

        // Assert
        assertFalse(unregistered);
    }

    @Test
    @DisplayName("Should check if subscriber is registered")
    void shouldCheckIfSubscriberIsRegistered() {
        // Arrange
        TestEventSubscriber subscriber = new TestEventSubscriber();
        registry.register(subscriber);

        // Act & Assert
        assertTrue(registry.isRegistered(subscriber));
        assertFalse(registry.isRegistered(new AnotherEventSubscriber()));
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
    @DisplayName("Should handle multiple subscribers for same event type")
    void shouldHandleMultipleSubscribersForSameEventType() {
        // Arrange
        TestEventSubscriber subscriber1 = new TestEventSubscriber();
        TestEventSubscriber subscriber2 = new TestEventSubscriber();

        // Act
        registry.register(subscriber1);
        registry.register(subscriber2);

        // Assert
        List<EventHandler> handlers = registry.getHandlers(new TestEvent("test"));
        assertEquals(2, handlers.size());
    }

    @Test
    @DisplayName("Should find subscriber for payload type when event is Envelope")
    void shouldFindSubscriberForPayloadTypeWhenEventIsEnvelope() {
        // Arrange: subscriber registered for TestEvent.class
        TestEventSubscriber subscriber = new TestEventSubscriber();
        registry.register(subscriber);

        // Act: dispatch Envelope<TestEvent>
        List<EventHandler> handlers = registry.getHandlers(Envelope.of(new TestEvent("test")));

        // Assert: subscriber should be found by payload type (TestEvent)
        assertEquals(1, handlers.size(),
                "Subscriber for TestEvent.class should be found when event is Envelope<TestEvent>");
        assertTrue(handlers.contains(subscriber),
                "The TestEventSubscriber should be in the handler list");
    }

    @Test
    @DisplayName("Generic Event subscriber should be found for Envelope event")
    void shouldFindGenericEventSubscriberForEnvelopeEvent() {
        // Arrange: generic subscriber for Event.class
        GenericEventSubscriber subscriber = new GenericEventSubscriber();
        registry.register(subscriber);

        // Act: dispatch Envelope<TestEvent>
        List<EventHandler> handlers = registry.getHandlers(Envelope.of(new TestEvent("test")));

        // Assert: generic subscriber should always be found
        assertEquals(1, handlers.size(),
                "Generic subscriber for Event.class should be found for Envelope<TestEvent>");
    }

    @Test
    @DisplayName("Should find subscriber registered for Envelope class directly")
    void shouldFindSubscriberForEnvelopeClassDirectly() {
        // Arrange: subscriber registered for Envelope.class
        var subscriber = new EnvelopeEventSubscriber();
        registry.register(subscriber);

        // Act: dispatch Envelope<TestEvent>
        List<EventHandler> handlers = registry.getHandlers(Envelope.of(new TestEvent("test")));

        // Assert: this works even before the fix
        assertEquals(1, handlers.size(),
                "Subscriber for Envelope.class should be found for Envelope event");
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
     * Test subscriber for specific event type.
     */
    private static final class TestEventSubscriber implements EventSubscriber {
        @Override
        public List<Class<?>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    /**
     * Test subscriber for another event type.
     */
    private static final class AnotherEventSubscriber implements EventSubscriber {
        @Override
        public List<Class<?>> events() {
            return List.of(AnotherEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    /**
     * Test subscriber for multiple event types.
     */
    private static final class MultiEventSubscriber implements EventSubscriber {
        @Override
        public List<Class<?>> events() {
            return List.of(TestEvent.class, AnotherEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    /**
     * Generic subscriber for all events.
     */
    private static final class GenericEventSubscriber implements EventSubscriber {
        @Override
        public List<Class<?>> events() {
            return List.of(Event.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    /**
     * Subscriber registered for Envelope class directly.
     */
    private static final class EnvelopeEventSubscriber implements EventSubscriber {
        @Override
        public List<Class<?>> events() {
            return List.of(Envelope.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }
}
