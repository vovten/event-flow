package com.github.vovten.eventflow.event.collection;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.event.EventListener;
import com.github.vovten.eventflow.event.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SpringEventListenerInterfaceCollection
 */
class SpringEventListenerInterfaceCollectionTest {

    private ExecutorService executorService;
    private ApplicationContext applicationContext;
    private SpringEventListenerInterfaceCollection collection;

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        applicationContext = mock(ApplicationContext.class);
    }

    @Test
    @DisplayName("Should add listener implementing EventListener interface")
    void shouldAddListenerImplementingEventListenerInterface() {
        // given
        collection = new SpringEventListenerInterfaceCollection(executorService);
        TestEventListener listener = new TestEventListener();

        // when
        collection.add(listener);

        // then
        assertEquals(1, collection.size());
        assertTrue(collection.contains(listener));
    }

    @Test
    @DisplayName("Should pass event to registered listener")
    void shouldPassEventToRegisteredListener() throws InterruptedException {
        // given
        collection = new SpringEventListenerInterfaceCollection(executorService);
        TestEventListener listener = new TestEventListener();
        collection.add(listener);
        TestEvent event = TestEvent.create("Test message");

        // when
        boolean result = collection.pass(event);

        // then
        Thread.sleep(100); // Wait for async execution
        assertTrue(result);
        assertTrue(listener.wasCalled());
        assertEquals("Test message", listener.getLastEvent().getMessage());
    }

    @Test
    @DisplayName("Should return false when no listeners registered")
    void shouldReturnFalseWhenNoListenersRegistered() {
        // given
        collection = new SpringEventListenerInterfaceCollection(executorService);
        TestEvent event = TestEvent.create();

        // when
        boolean result = collection.pass(event);

        // then
        assertFalse(result);
    }

    @Test
    @DisplayName("Should return false when event type has no listeners")
    void shouldReturnFalseWhenEventTypeHasNoListeners() {
        // given
        collection = new SpringEventListenerInterfaceCollection(executorService);
        TestEventListener listener = new TestEventListener();
        collection.add(listener);
        ExternalTestEvent event = new ExternalTestEvent();

        // when
        boolean result = collection.pass(event);

        // then
        assertFalse(result);
    }

    @Test
    @DisplayName("Should initialize listeners from application context")
    void shouldInitializeListenersFromApplicationContext() {
        // given
        TestEventListener listener = new TestEventListener();
        when(applicationContext.getBeansOfType(EventListener.class))
            .thenReturn(java.util.Map.of("testListener", listener));

        // when
        collection = new SpringEventListenerInterfaceCollection(executorService, applicationContext);

        // then
        assertEquals(1, collection.size());
        assertTrue(collection.contains(listener));
    }

    @Test
    @DisplayName("Should not support adding listener collection")
    void shouldNotSupportAddingListenerCollection() {
        // given
        collection = new SpringEventListenerInterfaceCollection(executorService);
        EventListenerCollection otherCollection = mock(EventListenerCollection.class);

        // when & then
        assertThrows(
            UnsupportedOperationException.class,
            () -> collection.add(otherCollection)
        );
    }

    @Test
    @DisplayName("Should handle multiple listeners for same event type")
    void shouldHandleMultipleListenersForSameEventType() throws InterruptedException {
        // given
        collection = new SpringEventListenerInterfaceCollection(executorService);
        TestEventListener listener1 = new TestEventListener();
        TestEventListener listener2 = new TestEventListener();
        collection.add(listener1);
        collection.add(listener2);
        TestEvent event = TestEvent.create();

        // when
        collection.pass(event);

        // then
        Thread.sleep(500);
        assertTrue(listener1.wasCalled(), "Listener 1 should be called");
        assertTrue(listener2.wasCalled(), "Listener 2 should be called");
    }

    @Test
    @DisplayName("Should return correct size")
    void shouldReturnCorrectSize() {
        // given
        collection = new SpringEventListenerInterfaceCollection(executorService);
        TestEventListener listener1 = new TestEventListener();
        TestEventListener listener2 = new TestEventListener();

        // when
        collection.add(listener1);
        collection.add(listener2);

        // then
        assertEquals(1, collection.size()); // Same event type, so size is 1
    }

    @Test
    @DisplayName("Should return true for isEmpty when no listeners")
    void shouldReturnTrueForIsEmptyWhenNoListeners() {
        // given
        collection = new SpringEventListenerInterfaceCollection(executorService);

        // then
        assertTrue(collection.isEmpty());
    }

    @Test
    @DisplayName("Should return false for contains when listener not registered")
    void shouldReturnFalseForContainsWhenListenerNotRegistered() {
        // given
        collection = new SpringEventListenerInterfaceCollection(executorService);
        TestEventListener listener = new TestEventListener();

        // then
        assertFalse(collection.contains(listener));
    }

    @Test
    @DisplayName("Should return false for contains when object is not EventListener")
    void shouldReturnFalseForContainsWhenObjectIsNotEventListener() {
        // given
        collection = new SpringEventListenerInterfaceCollection(executorService);
        Object notAListener = new Object();

        // then
        assertFalse(collection.contains(notAListener));
    }

    // Test helper class
    static class TestEventListener implements EventListener {
        private boolean called = false;
        private TestEvent lastEvent;

        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
            this.called = true;
            this.lastEvent = (TestEvent) event;
        }

        boolean wasCalled() {
            return called;
        }

        TestEvent getLastEvent() {
            return lastEvent;
        }
    }

    // Test event class for external events
    static class ExternalTestEvent implements Event {
        @Override
        public Class<? extends Event> type() {
            return ExternalTestEvent.class;
        }
    }
}
