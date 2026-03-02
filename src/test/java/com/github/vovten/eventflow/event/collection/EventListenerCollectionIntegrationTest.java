package com.github.vovten.eventflow.event.collection;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.event.EventFlowTestApplication;
import com.github.vovten.eventflow.event.EventListener;
import com.github.vovten.eventflow.event.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for EventListenerCollection implementations
 */
@SpringBootTest(classes = EventFlowTestApplication.class)
class EventListenerCollectionIntegrationTest {

    @Autowired
    private ApplicationContext applicationContext;

    private TestEvent lastEvent;
    private CountDownLatch latch;

    @BeforeEach
    void setUp() {
        lastEvent = null;
        latch = new CountDownLatch(1);
    }

    @Test
    @DisplayName("Should pass event to interface-based listener")
    void shouldPassEventToInterfaceBasedListener() throws InterruptedException {
        // given
        SpringEventListenerInterfaceCollection collection = 
            new SpringEventListenerInterfaceCollection(
                java.util.concurrent.Executors.newFixedThreadPool(2));
        InterfaceBasedListener listener = new InterfaceBasedListener();
        collection.add(listener);
        TestEvent event = TestEvent.create("Interface listener test");

        // when
        boolean result = collection.pass(event);
        Thread.sleep(100);

        // then
        assertTrue(result);
        assertTrue(listener.wasCalled());
        assertNotNull(listener.getLastEvent());
        assertEquals("Interface listener test", listener.getLastEvent().getMessage());
    }

    @Test
    @DisplayName("Should pass event to annotation-based listener")
    void shouldPassEventToAnnotationBasedListener() throws InterruptedException {
        // given
        SpringEventListenerAnnotationCollection collection = 
            new SpringEventListenerAnnotationCollection(
                java.util.concurrent.Executors.newFixedThreadPool(2));
        AnnotationBasedListener listener = new AnnotationBasedListener();
        collection.add(listener);
        TestEvent event = TestEvent.create("Annotation listener test");

        // when
        boolean result = collection.pass(event);
        Thread.sleep(100);

        // then
        assertTrue(result);
        assertTrue(listener.wasCalled());
        assertNotNull(listener.getLastEvent());
        assertEquals("Annotation listener test", listener.getLastEvent().getMessage());
    }

    @Test
    @DisplayName("Should pass event through composite collection")
    void shouldPassEventThroughCompositeCollection() throws InterruptedException {
        // given
        SpringEventListenerInterfaceCollection interfaceCollection = 
            new SpringEventListenerInterfaceCollection(
                java.util.concurrent.Executors.newFixedThreadPool(2));
        SpringEventListenerAnnotationCollection annotationCollection = 
            new SpringEventListenerAnnotationCollection(
                java.util.concurrent.Executors.newFixedThreadPool(2));
        
        InterfaceBasedListener interfaceListener = new InterfaceBasedListener();
        AnnotationBasedListener annotationListener = new AnnotationBasedListener();
        
        interfaceCollection.add(interfaceListener);
        annotationCollection.add(annotationListener);
        
        CompositeEventListenerCollection compositeCollection = 
            new CompositeEventListenerCollection(
                new java.util.ArrayList<>(List.of(interfaceCollection, annotationCollection)));
        
        TestEvent event = TestEvent.create("Composite listener test");

        // when
        boolean result = compositeCollection.pass(event);
        Thread.sleep(100);

        // then
        assertTrue(result);
        assertTrue(interfaceListener.wasCalled());
        assertTrue(annotationListener.wasCalled());
    }

    @Test
    @DisplayName("Should initialize listeners from Spring context")
    void shouldInitializeListenersFromSpringContext() throws InterruptedException {
        // given
        SpringEventListenerInterfaceCollection collection = 
            new SpringEventListenerInterfaceCollection(
                java.util.concurrent.Executors.newFixedThreadPool(2),
                applicationContext);
        
        SpringEventListenerAnnotationCollection annotationCollection = 
            new SpringEventListenerAnnotationCollection(
                "",
                java.util.concurrent.Executors.newFixedThreadPool(2),
                applicationContext);

        TestEvent event = TestEvent.create("Spring context test");

        // when
        boolean result1 = collection.pass(event);
        boolean result2 = annotationCollection.pass(event);
        Thread.sleep(100);

        // then
        // Collections should be initialized with beans from context
        assertTrue(result1 || result2);
    }

    // Interface-based listener
    static class InterfaceBasedListener implements EventListener {
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

    // Annotation-based listener
    @org.springframework.stereotype.Component
    static class AnnotationBasedListener {
        private boolean called = false;
        private TestEvent lastEvent;

        @com.github.vovten.eventflow.event.annotation.EventListener
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
}
