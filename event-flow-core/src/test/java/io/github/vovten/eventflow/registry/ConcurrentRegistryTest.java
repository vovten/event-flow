package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.EventListener;
import io.github.vovten.eventflow.EventSubscriber;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for thread safety of registry implementations.
 * @since 1.0.0
 */
@DisplayName("Concurrent Registry Tests")
class ConcurrentRegistryTest {

    @Test
    @DisplayName("EventListenerRegistry should handle concurrent register and getHandlers")
    void eventListenerRegistryConcurrentAccess() throws InterruptedException {
        EventListenerRegistry registry = new EventListenerRegistry();
        int threadCount = 10;
        int iterations = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        // Start threads that concurrently register handlers and get handlers
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterations; i++) {
                        // Register a listener
                        Object listener = new Object() {
                            @EventListener
                            public void handleTestEvent(TestEvent event) {
                                // Handle event
                            }

                            @EventListener
                            public void handleGenericEvent(Event event) {
                                // Handle generic event
                            }
                        };
                        registry.register(listener);

                        // Get handlers
                        TestEvent testEvent = new TestEvent("test-" + threadId + "-" + i, "msg");
                        List<EventHandler> handlers = registry.getHandlers(testEvent);
                        assertNotNull(handlers);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertTrue(completed, "Threads should complete within timeout");
        assertEquals(0, errors.get(), "No ConcurrentModificationException should occur");
    }

    @Test
    @DisplayName("EventSubscriberRegistry should handle concurrent register and getHandlers")
    void eventSubscriberRegistryConcurrentAccess() throws InterruptedException {
        EventSubscriberRegistry registry = new EventSubscriberRegistry();
        int threadCount = 10;
        int iterations = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterations; i++) {
                        // Register a subscriber
                        EventSubscriber subscriber = new EventSubscriber() {
                            @Override
                            public List<Class<?>> events() {
                                return List.of(TestEvent.class, Event.class);
                            }

                            @Override
                            public void onEvent(Event event) {
                                // Handle event
                            }
                        };
                        registry.register(subscriber);

                        // Get handlers
                        TestEvent testEvent = new TestEvent("test-" + threadId + "-" + i, "msg");
                        List<EventHandler> handlers = registry.getHandlers(testEvent);
                        assertNotNull(handlers);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertTrue(completed, "Threads should complete within timeout");
        assertEquals(0, errors.get(), "No ConcurrentModificationException should occur");
    }

    @Test
    @DisplayName("CompositeEventHandlerRegistry should handle concurrent merge and getHandlers")
    void compositeRegistryConcurrentMerge() throws InterruptedException {
        EventListenerRegistry registry1 = new EventListenerRegistry();
        EventSubscriberRegistry registry2 = new EventSubscriberRegistry();
        CompositeEventHandlerRegistry composite = new CompositeEventHandlerRegistry(
                List.of(registry1, registry2)
        );

        int threadCount = 5;
        int iterations = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterations; i++) {
                        // Merge a new registry
                        EventListenerRegistry newRegistry = new EventListenerRegistry();
                        composite.merge(newRegistry);

                        // Get handlers concurrently
                        TestEvent testEvent = new TestEvent("test-" + threadId + "-" + i, "msg");
                        List<EventHandler> handlers = composite.getHandlers(testEvent);
                        assertNotNull(handlers);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertTrue(completed, "Threads should complete within timeout");
        assertEquals(0, errors.get(), "No ConcurrentModificationException should occur");
    }

    @Test
    @DisplayName("All handlers should be found under concurrent access")
    void allHandlersFoundUnderConcurrency() throws InterruptedException {
        EventSubscriberRegistry registry = new EventSubscriberRegistry();
        int initialSubscribers = 10;
        int concurrentSubscribers = 10;

        // Pre-register some subscribers
        for (int i = 0; i < initialSubscribers; i++) {
            final int id = i;
            registry.register(new EventSubscriber() {
                @Override
                public List<Class<?>> events() {
                    return List.of(TestEvent.class);
                }

                @Override
                public void onEvent(Event event) {
                }
            });
        }

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch registerLatch = new CountDownLatch(concurrentSubscribers);
        CountDownLatch checkLatch = new CountDownLatch(concurrentSubscribers);
        ExecutorService executor = Executors.newFixedThreadPool(concurrentSubscribers * 2);
        AtomicInteger errors = new AtomicInteger(0);

        // Start threads that concurrently register and check handlers
        for (int i = 0; i < concurrentSubscribers; i++) {
            final int id = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    // Register new subscriber
                    registry.register(new EventSubscriber() {
                        @Override
                        public List<Class<?>> events() {
                            return List.of(TestEvent.class);
                        }

                        @Override
                        public void onEvent(Event event) {
                        }
                    });
                    registerLatch.countDown();

                    // Wait for all registrations to start
                    registerLatch.await();

                    // Check that we can find handlers
                    TestEvent testEvent = new TestEvent("test-" + id, "msg");
                    List<EventHandler> handlers = registry.getHandlers(testEvent);

                    // Should find at least initialSubscribers handlers
                    if (handlers.size() < initialSubscribers) {
                        errors.incrementAndGet();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    checkLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = checkLatch.await(30, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertTrue(completed, "Threads should complete within timeout");
        assertEquals(0, errors.get(), "All threads should find expected handlers");
    }
}
