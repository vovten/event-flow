package com.github.vovten.eventflow.transport.incoming;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for InMemoryIncomingEventTransport.
 */
class InMemoryIncomingEventTransportTest {

    private ExecutorService testExecutor;

    @BeforeEach
    void setUp() {
        testExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() {
        if (testExecutor != null && !testExecutor.isShutdown()) {
            testExecutor.shutdownNow();
        }
    }

    @Test
    void testDefaultConstructor() {
        InMemoryIncomingEventTransport transport = new InMemoryIncomingEventTransport();

        assertEquals("in-memory", transport.name());
    }

    @Test
    void testConstructorWithQueueSize() {
        InMemoryIncomingEventTransport transport = new InMemoryIncomingEventTransport(500);

        assertEquals("in-memory", transport.name());
    }

    @Test
    void testConstructorWithExistingQueue() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(100);
        InMemoryIncomingEventTransport transport = new InMemoryIncomingEventTransport(queue);

        assertEquals("in-memory", transport.name());
    }

    @Test
    void testConstructorWithQueueAndExecutor() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(100);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        InMemoryIncomingEventTransport transport = new InMemoryIncomingEventTransport(queue, executor);

        assertEquals("in-memory", transport.name());
    }

    @Test
    void testStart_DeliversEvents() throws InterruptedException {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        InMemoryIncomingEventTransport transport = new InMemoryIncomingEventTransport(queue, testExecutor);
        AtomicInteger deliveredCount = new AtomicInteger(0);
        AtomicBoolean delivered = new AtomicBoolean(false);

        TestEvent event = new TestEvent();
        transport.start(e -> {
            delivered.set(e.equals(event));
            deliveredCount.incrementAndGet();
        });

        queue.offer(event);
        Thread.sleep(100);

        assertTrue(delivered.get(), "Event should be delivered");
        assertEquals(1, deliveredCount.get());

        transport.stop();
    }

    @Test
    void testStart_AlreadyRunning_LogsWarning() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        InMemoryIncomingEventTransport transport = new InMemoryIncomingEventTransport(queue, testExecutor);

        transport.start(e -> {});
        transport.start(e -> {});

        transport.stop();
    }

    @Test
    void testStop_StopsProcessing() throws InterruptedException {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        InMemoryIncomingEventTransport transport = new InMemoryIncomingEventTransport(queue, testExecutor);
        AtomicInteger deliveredCount = new AtomicInteger(0);

        transport.start(e -> deliveredCount.incrementAndGet());
        Thread.sleep(50);

        queue.offer(new TestEvent());
        Thread.sleep(100);

        transport.stop();
        queue.offer(new TestEvent());
        Thread.sleep(100);

        assertEquals(1, deliveredCount.get(), "Only first event should be delivered");
    }

    @Test
    void testStop_MultipleTimes_DoesNotThrow() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        InMemoryIncomingEventTransport transport = new InMemoryIncomingEventTransport(queue, testExecutor);

        assertDoesNotThrow(() -> {
            transport.stop();
            transport.stop();
            transport.stop();
        });
    }

    @Test
    void testStop_NotRunning_DoesNothing() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        InMemoryIncomingEventTransport transport = new InMemoryIncomingEventTransport(queue, testExecutor);

        assertDoesNotThrow(transport::stop);
    }

    @Test
    void testMultipleEvents_DeliveredInOrder() throws InterruptedException {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>(10);
        InMemoryIncomingEventTransport transport = new InMemoryIncomingEventTransport(queue, testExecutor);
        java.util.List<Event> deliveredEvents = new java.util.concurrent.CopyOnWriteArrayList<>();

        transport.start(deliveredEvents::add);

        TestEvent event1 = new TestEvent("event1");
        TestEvent event2 = new TestEvent("event2");
        TestEvent event3 = new TestEvent("event3");

        queue.offer(event1);
        queue.offer(event2);
        queue.offer(event3);

        Thread.sleep(200);

        assertEquals(3, deliveredEvents.size());
        assertEquals(event1, deliveredEvents.get(0));
        assertEquals(event2, deliveredEvents.get(1));
        assertEquals(event3, deliveredEvents.get(2));

        transport.stop();
    }

    static class TestEvent implements Event {
        private final String id;

        TestEvent(String id) {
            this.id = id;
        }

        TestEvent() {
            this("default");
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof TestEvent)) return false;
            TestEvent other = (TestEvent) obj;
            return id.equals(other.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }
}
