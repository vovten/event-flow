package com.github.vovten.eventflow.collection;

import com.github.vovten.eventflow.annotation.EventListener;
import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for CompositeEventListenerCollection
 */
class CompositeEventListenerCollectionTest {

    private ExecutorService executorService;
    private CompositeEventListenerCollection compositeCollection;

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
    }

    @Test
    @DisplayName("Should compose multiple listener collections")
    void shouldComposeMultipleListenerCollections() {
        // given
        SpringEventListenerInterfaceCollection collection1 =
                new SpringEventListenerInterfaceCollection(executorService);
        SpringEventListenerAnnotationCollection collection2 =
                new SpringEventListenerAnnotationCollection(executorService);
        compositeCollection = new CompositeEventListenerCollection(
                new ArrayList<>(List.of(collection1, collection2)));

        // when & then
        assertTrue(compositeCollection.isEmpty());
        assertEquals(0, compositeCollection.size());
    }

    @Test
    @DisplayName("Should pass event to all composed collections")
    void shouldPassEventToAllComposedCollections() throws InterruptedException {
        // given
        SpringEventListenerInterfaceCollection interfaceCollection =
                new SpringEventListenerInterfaceCollection(executorService);
        TestEventListener listener = new TestEventListener();
        interfaceCollection.add(listener);

        SpringEventListenerAnnotationCollection annotationCollection =
                new SpringEventListenerAnnotationCollection(executorService);
        AnnotatedEventListener annotatedListener = new AnnotatedEventListener();
        annotationCollection.add(annotatedListener);

        compositeCollection = new CompositeEventListenerCollection(
                new ArrayList<>(List.of(interfaceCollection, annotationCollection)));

        TestEvent event = TestEvent.create("Test message");

        // when
        boolean result = compositeCollection.pass(event);

        // then
        Thread.sleep(100);
        assertTrue(result);
        assertTrue(listener.wasCalled());
        assertTrue(annotatedListener.wasCalled());
    }

    @Test
    @DisplayName("Should return false when no collections have listeners for event")
    void shouldReturnFalseWhenNoCollectionsHaveListenersForEvent() {
        // given
        SpringEventListenerInterfaceCollection collection1 =
                new SpringEventListenerInterfaceCollection(executorService);
        SpringEventListenerAnnotationCollection collection2 =
                new SpringEventListenerAnnotationCollection(executorService);
        compositeCollection = new CompositeEventListenerCollection(
                new ArrayList<>(List.of(collection1, collection2)));

        TestEvent event = TestEvent.create();

        // when
        boolean result = compositeCollection.pass(event);

        // then
        assertFalse(result);
    }

    @Test
    @DisplayName("Should add listener to all composed collections")
    void shouldAddListenerToAllComposedCollections() {
        // given
        SpringEventListenerInterfaceCollection collection1 =
                new SpringEventListenerInterfaceCollection(executorService);
        SpringEventListenerInterfaceCollection collection2 =
                new SpringEventListenerInterfaceCollection(executorService);
        compositeCollection = new CompositeEventListenerCollection(
                new ArrayList<>(List.of(collection1, collection2)));

        TestEventListener listener = new TestEventListener();

        // when
        compositeCollection.add(listener);

        // then
        assertTrue(collection1.contains(listener));
        assertTrue(collection2.contains(listener));
    }

    @Test
    @DisplayName("Should check contains in all composed collections")
    void shouldCheckContainsInAllComposedCollections() {
        // given
        SpringEventListenerInterfaceCollection collection1 =
                new SpringEventListenerInterfaceCollection(executorService);
        SpringEventListenerInterfaceCollection collection2 =
                new SpringEventListenerInterfaceCollection(executorService);
        compositeCollection = new CompositeEventListenerCollection(
                new ArrayList<>(List.of(collection1, collection2)));

        TestEventListener listener = new TestEventListener();
        collection1.add(listener);

        // when & then
        assertTrue(compositeCollection.contains(listener));
    }

    @Test
    @DisplayName("Should calculate total size from all collections")
    void shouldCalculateTotalSizeFromAllCollections() {
        // given
        SpringEventListenerInterfaceCollection collection1 =
                new SpringEventListenerInterfaceCollection(executorService);
        SpringEventListenerInterfaceCollection collection2 =
                new SpringEventListenerInterfaceCollection(executorService);

        TestEventListener listener1 = new TestEventListener();
        TestEventListener listener2 = new TestEventListener();
        collection1.add(listener1);
        collection2.add(listener2);

        compositeCollection = new CompositeEventListenerCollection(
                new ArrayList<>(List.of(collection1, collection2)));

        // when & then
        assertEquals(2, compositeCollection.size());
    }

    @Test
    @DisplayName("Should return false for isEmpty when any collection has listeners")
    void shouldReturnFalseForIsEmptyWhenAnyCollectionHasListeners() {
        // given
        SpringEventListenerInterfaceCollection collection1 =
                new SpringEventListenerInterfaceCollection(executorService);
        SpringEventListenerInterfaceCollection collection2 =
                new SpringEventListenerInterfaceCollection(executorService);

        TestEventListener listener = new TestEventListener();
        collection1.add(listener);

        compositeCollection = new CompositeEventListenerCollection(
                new ArrayList<>(List.of(collection1, collection2)));

        // when & then
        assertFalse(compositeCollection.isEmpty());
    }

    @Test
    @DisplayName("Should support adding listener collection")
    void shouldSupportAddingListenerCollection() {
        // given
        SpringEventListenerInterfaceCollection collection1 =
                new SpringEventListenerInterfaceCollection(executorService);
        TestEventListener listener = new TestEventListener();
        collection1.add(listener);
        
        compositeCollection = new CompositeEventListenerCollection(
                new ArrayList<>(List.of(collection1))
        );

        SpringEventListenerInterfaceCollection collection2 =
                new SpringEventListenerInterfaceCollection(executorService);
        TestEventListener listener2 = new TestEventListener();
        collection2.add(listener2);

        // when
        compositeCollection.add(collection2);

        // then
        assertEquals(2, compositeCollection.size());
    }

    // Test helper class
    static class TestEventListener implements com.github.vovten.eventflow.EventListener {
        private boolean called = false;

        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
            this.called = true;
        }

        boolean wasCalled() {
            return called;
        }
    }

    // Test helper class with @EventListener annotation
    static class AnnotatedEventListener {
        private boolean called = false;

        @EventListener
        public void handleTestEvent(TestEvent event) {
            this.called = true;
        }

        boolean wasCalled() {
            return called;
        }
    }
}
