package io.github.vovten.eventflow.benchmark;

import io.github.vovten.eventflow.EventListener;
import io.github.vovten.eventflow.dispatcher.EventDispatcher;
import io.github.vovten.eventflow.dispatcher.IdempotentEventDispatcher;
import io.github.vovten.eventflow.dispatcher.LoggingEventDispatcher;
import io.github.vovten.eventflow.dispatcher.UnifiedEventDispatcher;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.publisher.LoggingEventPublisher;
import io.github.vovten.eventflow.publisher.RetryEventPublisher;
import io.github.vovten.eventflow.registry.EventListenerRegistry;
import io.github.vovten.eventflow.transport.SendResults;
import io.github.vovten.eventflow.transport.incoming.LocalQueueInTransport;
import io.github.vovten.eventflow.transport.outgoing.LocalQueueOutTransport;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Performance benchmark for LocalQueue transport with decorators.
 * Run manually: mvn test -pl event-flow-core -Dtest=DecoratorBenchmarkTest -DskipTests=false
 */
@Disabled("Run manually: mvn test -pl event-flow-core -Dtest=DecoratorBenchmarkTest -DskipTests=false")
@DisplayName("Decorator Benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DecoratorBenchmarkTest {

    private static final int EVENT_COUNT = 1_000_000;
    private ExecutorService dispatcherExecutor;

    static {
        BenchmarkLogConfig.configureForBenchmarks();
    }

    @AfterEach
    void tearDown() {
        if (dispatcherExecutor != null) {
            dispatcherExecutor.shutdownNow();
        }
    }

    // =====================================================
    // Publisher Decorator Tests
    // =====================================================

    @Nested
    @DisplayName("Publisher Decorators")
    class PublisherDecorators {

        @Test
        @DisplayName("Baseline: No publisher decorator")
        void noPublisherDecorator() throws Exception {
            runBenchmark("Publisher: NONE", null, null);
        }

        @Test
        @DisplayName("LoggingEventPublisher")
        void withLoggingEventPublisher() throws Exception {
            runBenchmark("Publisher: LoggingEventPublisher", 
                base -> new LoggingEventPublisher(base, 1024), null);
        }

        @Test
        @DisplayName("RetryEventPublisher")
        void withRetryEventPublisher() throws Exception {
            runBenchmark("Publisher: RetryEventPublisher (3 retries)", 
                base -> new RetryEventPublisher(base, 3, Duration.ofMillis(10), 2), null);
        }

        @Test
        @DisplayName("LoggingEventPublisher + RetryEventPublisher")
        void withLoggingAndRetryPublisher() throws Exception {
            runBenchmark("Publisher: Logging + Retry", 
                base -> new RetryEventPublisher(new LoggingEventPublisher(base, 1024), 3, Duration.ofMillis(10), 2), null);
        }
    }

    // =====================================================
    // Dispatcher Decorator Tests
    // =====================================================

    @Nested
    @DisplayName("Dispatcher Decorators")
    class DispatcherDecorators {

        @Test
        @DisplayName("Baseline: No dispatcher decorator")
        void noDispatcherDecorator() throws Exception {
            runBenchmark("Dispatcher: NONE", null, null);
        }

        @Test
        @DisplayName("LoggingEventDispatcher")
        void withLoggingEventDispatcher() throws Exception {
            runBenchmark("Dispatcher: LoggingEventDispatcher", 
                null, base -> new LoggingEventDispatcher(base));
        }

        @Test
        @DisplayName("IdempotentEventDispatcher")
        void withIdempotentEventDispatcher() throws Exception {
            runBenchmark("Dispatcher: IdempotentEventDispatcher (10s TTL, 100k cache)", 
                null, base -> new IdempotentEventDispatcher(
                    base, Duration.ofSeconds(10), 100_000, false));
        }

        @Test
        @DisplayName("LoggingEventDispatcher + IdempotentEventDispatcher")
        void withLoggingAndIdempotentDispatcher() throws Exception {
            runBenchmark("Dispatcher: Logging + Idempotent", 
                null, base -> new IdempotentEventDispatcher(
                    new LoggingEventDispatcher(base), Duration.ofSeconds(10), 100_000, false));
        }
    }

    // =====================================================
    // Combined Decorator Tests
    // =====================================================

    @Nested
    @DisplayName("Combined Decorators")
    class CombinedDecorators {

        @Test
        @DisplayName("Logging publisher + Logging dispatcher")
        void withLoggingBoth() throws Exception {
            runBenchmark("Both: LoggingEventPublisher + LoggingEventDispatcher", 
                base -> new LoggingEventPublisher(base, 1024),
                base -> new LoggingEventDispatcher(base));
        }

        @Test
        @DisplayName("All decorators: Logging + Retry + Idempotent")
        void withAllDecorators() throws Exception {
            runBenchmark("All: Logging + Retry + Idempotent", 
                base -> new RetryEventPublisher(new LoggingEventPublisher(base, 1024), 3, Duration.ofMillis(10), 2),
                base -> new IdempotentEventDispatcher(
                    new LoggingEventDispatcher(base), 
                    Duration.ofSeconds(10), 100_000, false));
        }
    }

    // =====================================================
    // Benchmark Execution
    // =====================================================

    private void runBenchmark(
            String name,
            PublisherDecorator publisherDecorator,
            DispatcherDecorator dispatcherDecorator) throws Exception {

        System.out.println("\n" + "═".repeat(64));
        System.out.println("  " + name);
        System.out.println("═".repeat(64));
        System.out.printf("  Events:     %,d%n", EVENT_COUNT);
        System.out.println("═".repeat(64));

        // Create shared queue
        LinkedBlockingDeque<Event> sharedQueue = new LinkedBlockingDeque<>(EVENT_COUNT * 2);

        // Create base publisher
        LocalQueueOutTransport outTransport = new LocalQueueOutTransport(sharedQueue);
        EventPublisher basePublisher = event -> outTransport.send(event)
            .thenApply(r -> SendResults.of(List.of(r)));

        // Apply publisher decorator if specified
        EventPublisher publisher = publisherDecorator != null 
            ? publisherDecorator.apply(basePublisher) 
            : basePublisher;

        // Create registry and handler
        BenchmarkEventHandler handler = new BenchmarkEventHandler();
        EventListenerRegistry registry = new EventListenerRegistry();
        registry.register(handler);

        // Create base dispatcher
        LocalQueueInTransport inTransport = new LocalQueueInTransport(sharedQueue);
        dispatcherExecutor = Executors.newVirtualThreadPerTaskExecutor();
        UnifiedEventDispatcher baseDispatcher = new UnifiedEventDispatcher(
            dispatcherExecutor, registry, List.of(inTransport)
        );

        // Apply dispatcher decorator if specified
        EventDispatcher dispatcher = dispatcherDecorator != null
            ? dispatcherDecorator.apply(baseDispatcher)
            : baseDispatcher;

        dispatcher.start(event -> dispatcher.dispatch(event));

        // Wait for dispatcher to be ready
        Thread.sleep(500);

        CountDownLatch latch = new CountDownLatch(EVENT_COUNT);
        handler.setLatch(latch);
        handler.reset();

        long startTime = System.nanoTime();

        // Fire-and-forget for max throughput
        for (int i = 0; i < EVENT_COUNT; i++) {
            publisher.publish(new BenchmarkEvent("id-" + i, "payload-" + i));
        }

        long publishEndTime = System.nanoTime();

        boolean completed = latch.await(300, TimeUnit.SECONDS);
        long endTime = System.nanoTime();

        dispatcher.stop();

        assertEquals(EVENT_COUNT, handler.getProcessedCount(), "All events should be processed");

        double totalDuration = (endTime - startTime) / 1_000_000_000.0;
        double publishDuration = (publishEndTime - startTime) / 1_000_000_000.0;
        double processDuration = (endTime - publishEndTime) / 1_000_000_000.0;
        double throughput = EVENT_COUNT / totalDuration;

        System.out.println("─".repeat(64));
        System.out.printf("  Received:   %,12d%n", handler.getProcessedCount());
        System.out.printf("  Throughput: %12.0f events/sec%n", throughput);
        System.out.printf("  Publish:    %12.3f seconds%n", publishDuration);
        System.out.printf("  Process:    %12.3f seconds%n", processDuration);
        System.out.println("═".repeat(64));

        assertTrue(completed, "All events should be processed within timeout");
    }

    // =====================================================
    // Helper Interfaces
    // =====================================================

    @FunctionalInterface
    interface PublisherDecorator {
        EventPublisher apply(EventPublisher base);
    }

    @FunctionalInterface
    interface DispatcherDecorator {
        EventDispatcher apply(EventDispatcher base);
    }

    // =====================================================
    // Test Classes
    // =====================================================

    public static class BenchmarkEvent extends AbstractTraceableEvent {
        private String id;
        private String message;

        public BenchmarkEvent() { super(); }
        public BenchmarkEvent(String id, String message) { 
            super(); 
            this.id = id; 
            this.message = message; 
        }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }

        @Override
        public Class<?> type() { return BenchmarkEvent.class; }
    }

    public static class BenchmarkEventHandler {
        private final AtomicLong processedCount = new AtomicLong(0);
        private CountDownLatch latch;

        public void setLatch(CountDownLatch latch) { this.latch = latch; }
        public void reset() { processedCount.set(0); }
        public long getProcessedCount() { return processedCount.get(); }

        @EventListener
        public void onBenchmarkEvent(BenchmarkEvent event) {
            processedCount.incrementAndGet();
            if (latch != null) latch.countDown();
        }
    }
}