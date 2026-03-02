package com.github.vovten.eventflow.event.collection;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.event.test.TestEvent;
import com.github.vovten.eventflow.event.annotation.EventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SpringEventListenerAnnotationCollection
 */
class SpringEventListenerAnnotationCollectionTest {

    private ExecutorService executorService;
    private ApplicationContext applicationContext;
    private SpringEventListenerAnnotationCollection collection;

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        applicationContext = mock(ApplicationContext.class);
    }

    @Test
    @DisplayName("Should add listener with @EventListener annotation")
    void shouldAddListenerWithEventListenerAnnotation() {
        // given
        collection = new SpringEventListenerAnnotationCollection(executorService);
        AnnotatedEventListener listener = new AnnotatedEventListener();

        // when
        collection.add(listener);

        // then
        assertTrue(collection.contains(listener));
    }

    @Test
    @DisplayName("Should pass event to annotated listener method")
    void shouldPassEventToAnnotatedListenerMethod() throws InterruptedException {
        // given
        collection = new SpringEventListenerAnnotationCollection(executorService);
        AnnotatedEventListener listener = new AnnotatedEventListener();
        collection.add(listener);
        TestEvent event = TestEvent.create("Test message");

        // when
        boolean result = collection.pass(event);

        // then
        Thread.sleep(100);
        assertTrue(result);
        assertTrue(listener.wasCalled());
        assertEquals("Test message", listener.getLastEvent().getMessage());
    }

    @Test
    @DisplayName("Should initialize listeners from application context")
    void shouldInitializeListenersFromApplicationContext() {
        // given
        AnnotatedEventListener listener = new AnnotatedEventListener();
        when(applicationContext.getBeanDefinitionNames())
            .thenReturn(new String[]{"annotatedEventListener"});
        when(applicationContext.getBean("annotatedEventListener")).thenReturn(listener);

        // when
        collection = new SpringEventListenerAnnotationCollection(
            "", executorService, applicationContext);

        // then
        assertTrue(collection.contains(listener));
    }

    @Test
    @DisplayName("Should filter beans by scan package")
    void shouldFilterBeansByScanPackage() {
        // given
        AnnotatedEventListener listener = new AnnotatedEventListener();
        when(applicationContext.getBeanDefinitionNames())
            .thenReturn(new String[]{"annotatedEventListener"});
        when(applicationContext.getBean("annotatedEventListener")).thenReturn(listener);

        // when
        collection = new SpringEventListenerAnnotationCollection(
            "com.github.vovten", executorService, applicationContext);

        // then
        assertTrue(collection.contains(listener));
    }

    @Test
    @DisplayName("Should throw exception for invalid method signature")
    void shouldThrowExceptionForInvalidMethodSignature() {
        // given
        InvalidEventListener listener = new InvalidEventListener();

        // when & then
        IllegalEventListenerMethodSignatureException exception = assertThrows(
            IllegalEventListenerMethodSignatureException.class,
            () -> collection.add(listener)
        );
        assertTrue(exception.getMessage().contains("Method signature"));
    }

    @Test
    @DisplayName("Should return false when no listeners registered")
    void shouldReturnFalseWhenNoListenersRegistered() {
        // given
        collection = new SpringEventListenerAnnotationCollection(executorService);
        TestEvent event = TestEvent.create();

        // when
        boolean result = collection.pass(event);

        // then
        assertFalse(result);
    }

    @Test
    @DisplayName("Should handle Event.class as parameter type")
    void shouldHandleEventClassAsParameterType() throws InterruptedException {
        // given
        collection = new SpringEventListenerAnnotationCollection(executorService);
        GenericEventListener listener = new GenericEventListener();
        collection.add(listener);
        TestEvent event = TestEvent.create();

        // when
        boolean result = collection.pass(event);

        // then
        Thread.sleep(100);
        assertTrue(result);
        assertTrue(listener.wasCalled());
    }

    @Test
    @DisplayName("Should not support adding listener collection")
    void shouldNotSupportAddingListenerCollection() {
        // given
        collection = new SpringEventListenerAnnotationCollection(executorService);
        EventListenerCollection otherCollection = mock(EventListenerCollection.class);

        // when & then
        assertThrows(
            UnsupportedOperationException.class,
            () -> collection.add(otherCollection)
        );
    }

    @Test
    @DisplayName("Should return correct size")
    void shouldReturnCorrectSize() {
        // given
        collection = new SpringEventListenerAnnotationCollection(executorService);
        AnnotatedEventListener listener1 = new AnnotatedEventListener();
        AnnotatedEventListener listener2 = new AnnotatedEventListener();

        // when
        collection.add(listener1);
        collection.add(listener2);

        // then
        assertEquals(1, collection.size());
    }

    @Test
    @DisplayName("Should return true for isEmpty when no listeners")
    void shouldReturnTrueForIsEmptyWhenNoListeners() {
        // given
        collection = new SpringEventListenerAnnotationCollection(executorService);

        // then
        assertTrue(collection.isEmpty());
    }

    // Test helper class with @EventListener annotation
    static class AnnotatedEventListener {
        private boolean called = false;
        private TestEvent lastEvent;

        @EventListener
        public void handleTestEvent(TestEvent event) {
            this.called = true;
            this.lastEvent = event;
        }

        boolean wasCalled() {
            return called;
        }

        TestEvent getLastEvent() {
            return lastEvent;
        }
    }

    // Test helper class with invalid method signature
    static class InvalidEventListener {
        @EventListener
        public void handleEvent(String invalidParam) {
            // Invalid - should accept Event, not String
        }
    }

    // Test helper class that handles generic Event
    static class GenericEventListener {
        private boolean called = false;

        @EventListener
        public void handleEvent(Event event) {
            this.called = true;
        }

        boolean wasCalled() {
            return called;
        }
    }
}
