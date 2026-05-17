package io.github.vovten.eventflow.benchmark;

import io.github.vovten.eventflow.EventListener;
import io.github.vovten.eventflow.dispatcher.UnifiedEventDispatcher;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.registry.EventListenerRegistry;
import io.github.vovten.eventflow.transport.SendResults;
import io.github.vovten.eventflow.transport.incoming.LocalQueueInTransport;
import io.github.vovten.eventflow.transport.outgoing.LocalQueueOutTransport;
import org.junit.jupiter.api.*;

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
 * Performance benchmark for LocalQueue transport.
 * Run manually: mvn test -pl event-flow-core -Dtest=mvn test -pl event-flow-core -Dtest=LocalQueueBenchmarkTest -DskipTests=false
 */
@Disabled("Run manually: mvn test -pl event-flow-core -Dtest=LocalQueueBenchmarkTest -DskipTests=false")
@DisplayName("LocalQueue Benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LocalQueueBenchmarkTest {

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

    @Test
    @DisplayName("1M events throughput benchmark")
    void shouldHandle1MEvents() throws Exception {
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║          LocalQueue 1M Events Benchmark                      ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.printf("║ Events:     %,d                                      ║%n", EVENT_COUNT);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        // SHARED queue - critical!
        LinkedBlockingDeque<io.github.vovten.eventflow.event.Event> sharedQueue =
                new LinkedBlockingDeque<>(EVENT_COUNT * 2);

        // OUT transport with shared queue
        LocalQueueOutTransport outTransport = new LocalQueueOutTransport(sharedQueue);
        EventPublisher publisher = event -> outTransport.send(event)
                .thenApply(r -> SendResults.of(List.of(r)));

        // Handler using @EventListener
        BenchmarkEventHandler handler = new BenchmarkEventHandler();
        EventListenerRegistry registry = new EventListenerRegistry();
        registry.register(handler);

        // IN transport with SAME shared queue
        LocalQueueInTransport inTransport = new LocalQueueInTransport(sharedQueue);
        dispatcherExecutor = Executors.newVirtualThreadPerTaskExecutor();
        UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                dispatcherExecutor, registry, List.of(inTransport)
        );
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
        System.out.printf("  Published %,d events in %.2f seconds%n",
                EVENT_COUNT, (publishEndTime - startTime) / 1_000_000_000.0);

        dispatcher.stop();

        boolean completed = latch.await(300, TimeUnit.SECONDS);
        long endTime = System.nanoTime();

        assertEquals(EVENT_COUNT, handler.getProcessedCount(), "All events should be processed");

        double totalDuration = (endTime - startTime) / 1_000_000_000.0;
        double processDuration = (endTime - publishEndTime) / 1_000_000_000.0;
        double throughput = EVENT_COUNT / totalDuration;

        printResult(handler, throughput, totalDuration, processDuration);

        assertTrue(completed, "All events should be processed within timeout");
    }

    private void printResult(BenchmarkEventHandler handler, double throughput, double totalDuration, double processDuration) {
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║                    Results                                   ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.printf("║ Received:   %,12d                                      ║%n", handler.getProcessedCount());
        System.out.printf("║ Throughput: %12.0f events/sec                         ║%n", throughput);
        System.out.printf("║ Total time: %12.3f seconds                             ║%n", totalDuration);
        System.out.printf("║ Process:    %12.3f seconds                             ║%n", processDuration);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    static class BenchmarkEvent extends AbstractTraceableEvent {
        private String id;
        private String message;

        BenchmarkEvent() {
            super();
        }

        BenchmarkEvent(String id, String message) {
            super();
            this.id = id;
            this.message = message;
        }

        String getId() {
            return id;
        }

        void setId(String id) {
            this.id = id;
        }

        String getMessage() {
            return message;
        }

        void setMessage(String message) {
            this.message = message;
        }

        @Override
        public Class<?> type() {
            return BenchmarkEvent.class;
        }
    }

    public static class BenchmarkEventHandler {
        private final AtomicLong processedCount = new AtomicLong(0);
        private CountDownLatch latch;

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        public void reset() {
            processedCount.set(0);
        }

        public long getProcessedCount() {
            return processedCount.get();
        }

        @EventListener
        public void onBenchmarkEvent(BenchmarkEvent event) {
            processedCount.incrementAndGet();
            if (latch != null) {
                latch.countDown();
            }
        }
    }
}