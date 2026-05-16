package io.github.vovten.eventflow.benchmark;

import io.github.vovten.eventflow.EventListener;
import io.github.vovten.eventflow.dispatcher.UnifiedEventDispatcher;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.registry.EventListenerRegistry;
import io.github.vovten.eventflow.transport.incoming.LocalQueueInTransport;
import io.github.vovten.eventflow.transport.outgoing.LocalQueueOutTransport;
import org.junit.jupiter.api.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Performance benchmark for event-flow system.
 * Tests throughput on different transport types.
 */
@DisplayName("EventFlow Benchmark (DISABLED)")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EventFlowBenchmarkTest {

    private ExecutorService dispatcherExecutor;

    @AfterEach
    void tearDown() {
        if (dispatcherExecutor != null) {
            dispatcherExecutor.shutdownNow();
        }
    }

    @Nested
    @DisplayName("LocalQueue Transport Benchmark")
    class LocalQueueBenchmark {

        @Test
        @DisplayName("Should handle 100k events with shared queue")
        void shouldHandle100kEventsWithSharedQueue() throws Exception {
            int eventCount = 100_000;

            // SHARED queue - critical!
            LinkedBlockingDeque<Event> sharedQueue = new LinkedBlockingDeque<>(eventCount * 2);

            // Publisher uses OUT transport with shared queue
            LocalQueueOutTransport outTransport = new LocalQueueOutTransport(sharedQueue);
            EventPublisher publisher = event -> outTransport.send(event)
                    .thenApply(r -> io.github.vovten.eventflow.transport.SendResults.of(java.util.List.of(r)));

            // Handler using @EventListener
            BenchmarkEventHandler handler = new BenchmarkEventHandler();

            // Registry
            EventListenerRegistry registry = new EventListenerRegistry();
            registry.register(handler);

            // IN transport with SAME shared queue
            LocalQueueInTransport inTransport = new LocalQueueInTransport(sharedQueue);
            dispatcherExecutor = Executors.newVirtualThreadPerTaskExecutor();
            UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                    dispatcherExecutor, registry, java.util.List.of(inTransport)
            );
            dispatcher.start(event -> dispatcher.dispatch(event));

            // Benchmark
            CountDownLatch latch = new CountDownLatch(eventCount);
            handler.setLatch(latch);
            handler.reset();

            long startTime = System.nanoTime();

            // Fire-and-forget for max throughput
            for (int i = 0; i < eventCount; i++) {
                publisher.publish(new BenchmarkEvent("id-" + i, "payload-" + i));
            }

            long publishEndTime = System.nanoTime();

            boolean completed = latch.await(120, TimeUnit.SECONDS);
            long endTime = System.nanoTime();

            dispatcher.stop();

            assertEquals(eventCount, handler.getProcessedCount(), "All events should be processed");

            double totalDuration = (endTime - startTime) / 1_000_000_000.0;
            double publishDuration = (publishEndTime - startTime) / 1_000_000_000.0;
            double processDuration = (endTime - publishEndTime) / 1_000_000_000.0;
            double throughput = eventCount / totalDuration;

            System.out.println("\n========================================");
            System.out.println("LocalQueue Throughput Benchmark");
            System.out.println("========================================");
            System.out.printf("Total events:       %,d%n", eventCount);
            System.out.printf("Total duration:      %.3f seconds%n", totalDuration);
            System.out.printf("Publish duration:   %.3f seconds%n", publishDuration);
            System.out.printf("Process duration:    %.3f seconds%n", processDuration);
            System.out.printf("Throughput:          %,.0f events/second%n", throughput);
            System.out.println("========================================\n");

            assertTrue(completed, "All events should be processed within timeout");
        }

        @Test
        @DisplayName("Should measure publish latency")
        void shouldMeasurePublishLatency() throws Exception {
            int warmup = 1_000;
            int measure = 10_000;

            LinkedBlockingDeque<Event> sharedQueue = new LinkedBlockingDeque<>(measure * 2);
            LocalQueueOutTransport outTransport = new LocalQueueOutTransport(sharedQueue);
            EventPublisher publisher = event -> outTransport.send(event)
                    .thenApply(r -> io.github.vovten.eventflow.transport.SendResults.of(java.util.List.of(r)));

            BenchmarkEventHandler handler = new BenchmarkEventHandler();
            EventListenerRegistry registry = new EventListenerRegistry();
            registry.register(handler);

            LocalQueueInTransport inTransport = new LocalQueueInTransport(sharedQueue);
            dispatcherExecutor = Executors.newVirtualThreadPerTaskExecutor();
            UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                    dispatcherExecutor, registry, java.util.List.of(inTransport)
            );
            dispatcher.start(event -> dispatcher.dispatch(event));

            CountDownLatch latch = new CountDownLatch(warmup + measure);
            handler.setLatch(latch);

            // Warmup
            for (int i = 0; i < warmup; i++) {
                publisher.publish(new BenchmarkEvent("warmup-" + i, "data")).join();
            }
            latch.await(30, TimeUnit.SECONDS);

            // Measure
            handler.reset();
            long[] latencies = new long[measure];
            for (int i = 0; i < measure; i++) {
                long start = System.nanoTime();
                publisher.publish(new BenchmarkEvent("event-" + i, "data")).join();
                latencies[i] = System.nanoTime() - start;
            }

            dispatcher.stop();

            printLatencyStats("LocalQueue Publish Latency (join)", latencies, measure);
        }

        @Test
        @DisplayName("Should handle concurrent events")
        void shouldHandleConcurrentEvents() throws Exception {
            int eventCount = 50_000;
            int threadCount = 50;

            LinkedBlockingDeque<Event> sharedQueue = new LinkedBlockingDeque<>(eventCount * 2);
            LocalQueueOutTransport outTransport = new LocalQueueOutTransport(sharedQueue);
            EventPublisher publisher = event -> outTransport.send(event)
                    .thenApply(r -> io.github.vovten.eventflow.transport.SendResults.of(java.util.List.of(r)));

            BenchmarkEventHandler handler = new BenchmarkEventHandler();
            EventListenerRegistry registry = new EventListenerRegistry();
            registry.register(handler);

            LocalQueueInTransport inTransport = new LocalQueueInTransport(sharedQueue);
            dispatcherExecutor = Executors.newVirtualThreadPerTaskExecutor();
            UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                    dispatcherExecutor, registry, java.util.List.of(inTransport)
            );
            dispatcher.start(event -> dispatcher.dispatch(event));

            handler.reset();
            CountDownLatch processingLatch = new CountDownLatch(eventCount);
            handler.setLatch(processingLatch);

            ExecutorService publishExecutor = Executors.newVirtualThreadPerTaskExecutor();
            long startTime = System.nanoTime();

            // Publish from multiple virtual threads
            CountDownLatch publishLatch = new CountDownLatch(eventCount);
            for (int i = 0; i < eventCount; i++) {
                final int eventId = i;
                publishExecutor.submit(() -> {
                    publisher.publish(new BenchmarkEvent("id-" + eventId, "data-" + eventId));
                    publishLatch.countDown();
                });
            }

            // Wait for all events to be published
            boolean published = publishLatch.await(30, TimeUnit.SECONDS);
            long publishEndTime = System.nanoTime();

            assertTrue(published, "All events should be published");

            // Wait for all events to be processed
            boolean processed = processingLatch.await(120, TimeUnit.SECONDS);
            long endTime = System.nanoTime();

            dispatcher.stop();
            publishExecutor.shutdownNow();

            assertEquals(eventCount, handler.getProcessedCount(), "All events should be processed");

            double totalDuration = (endTime - startTime) / 1_000_000_000.0;
            double throughput = eventCount / totalDuration;

            System.out.println("\n========================================");
            System.out.println("LocalQueue Concurrent Benchmark");
            System.out.println("========================================");
            System.out.printf("Total events:       %,d%n", eventCount);
            System.out.printf("Concurrent threads:  %d%n", threadCount);
            System.out.printf("Total duration:      %.3f seconds%n", totalDuration);
            System.out.printf("Throughput:          %,.0f events/second%n", throughput);
            System.out.println("========================================\n");
        }
    }

    private void printLatencyStats(String title, long[] latencies, int count) {
        long sum = 0;
        long min = Long.MAX_VALUE;
        long max = 0;
        for (long l : latencies) {
            sum += l;
            if (l < min) min = l;
            if (l > max) max = l;
        }

        long[] sorted = latencies.clone();
        java.util.Arrays.sort(sorted);

        double avg = sum / (double) count / 1_000_000.0;
        double p50 = sorted[count / 2] / 1_000_000.0;
        double p95 = sorted[(int) (count * 0.95)] / 1_000_000.0;
        double p99 = sorted[(int) (count * 0.99)] / 1_000_000.0;

        System.out.println("\n========================================");
        System.out.println(title);
        System.out.println("========================================");
        System.out.printf("Events measured:    %,d%n", count);
        System.out.printf("Min:                %.3f ms%n", min / 1_000_000.0);
        System.out.printf("Avg:                %.3f ms%n", avg);
        System.out.printf("P50:                %.3f ms%n", p50);
        System.out.printf("P95:                %.3f ms%n", p95);
        System.out.printf("P99:                %.3f ms%n", p99);
        System.out.printf("Max:                %.3f ms%n", max / 1_000_000.0);
        System.out.println("========================================\n");
    }

    public static class BenchmarkEvent extends AbstractTraceableEvent {
        private final String id;
        private final String message;

        public BenchmarkEvent(String id, String message) {
            super();
            this.id = id;
            this.message = message;
        }

        public String getId() { return id; }
        public String getMessage() { return message; }

        @Override
        public Class<?> type() { return BenchmarkEvent.class; }
    }

    // IMPORTANT: Use @EventListener annotation, not EventHandler interface!
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