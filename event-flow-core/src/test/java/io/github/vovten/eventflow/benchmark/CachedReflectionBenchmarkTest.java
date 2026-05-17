package io.github.vovten.eventflow.benchmark;

import io.github.vovten.eventflow.EventListener;
import io.github.vovten.eventflow.dispatcher.UnifiedEventDispatcher;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.publisher.LoggingEventPublisher;
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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Benchmark for LoggingEventPublisher with cached reflection.
 * Run: mvn test -pl event-flow-core -Dtest=CachedReflectionBenchmarkTest -DskipTests=false -Dcheckstyle.skip=true
 */
@DisplayName("Cached Reflection Benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CachedReflectionBenchmarkTest {

    private static final int EVENT_COUNT = 500_000;
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
    @DisplayName("Baseline: No logging")
    void baseline() throws Exception {
        runBenchmark("Baseline (no logging)", null);
    }

    @Test
    @DisplayName("LoggingEventPublisher with cached reflection")
    void withLogging() throws Exception {
        runBenchmark("LoggingEventPublisher (cached reflection)", 
            base -> new LoggingEventPublisher(base, 1024));
    }

    private void runBenchmark(String name, java.util.function.Function<EventPublisher, EventPublisher> decoratorFn) throws Exception {
        System.out.println("\n" + "═".repeat(64));
        System.out.println("  " + name);
        System.out.println("═".repeat(64));

        LinkedBlockingDeque<io.github.vovten.eventflow.event.Event> sharedQueue = 
            new LinkedBlockingDeque<>(EVENT_COUNT * 2);

        LocalQueueOutTransport outTransport = new LocalQueueOutTransport(sharedQueue);
        EventPublisher basePublisher = event -> outTransport.send(event)
            .thenApply(r -> SendResults.of(List.of(r)));

        EventPublisher publisher = decoratorFn != null 
            ? decoratorFn.apply(basePublisher) 
            : basePublisher;

        BenchmarkEventHandler handler = new BenchmarkEventHandler();
        EventListenerRegistry registry = new EventListenerRegistry();
        registry.register(handler);

        LocalQueueInTransport inTransport = new LocalQueueInTransport(sharedQueue);
        dispatcherExecutor = Executors.newVirtualThreadPerTaskExecutor();
        UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
            dispatcherExecutor, registry, List.of(inTransport)
        );
        dispatcher.start(event -> dispatcher.dispatch(event));

        Thread.sleep(500);

        CountDownLatch latch = new CountDownLatch(EVENT_COUNT);
        handler.setLatch(latch);
        handler.reset();

        long startTime = System.nanoTime();

        for (int i = 0; i < EVENT_COUNT; i++) {
            publisher.publish(new BenchmarkEvent("id-" + i, "msg-" + i));
        }

        long publishEndTime = System.nanoTime();
        boolean completed = latch.await(120, TimeUnit.SECONDS);
        long endTime = System.nanoTime();

        dispatcher.stop();

        assertEquals(EVENT_COUNT, handler.getProcessedCount());

        double totalDuration = (endTime - startTime) / 1_000_000_000.0;
        double publishDuration = (publishEndTime - startTime) / 1_000_000_000.0;
        double processDuration = (endTime - publishEndTime) / 1_000_000_000.0;
        double throughput = EVENT_COUNT / totalDuration;

        System.out.println("─".repeat(64));
        System.out.printf("  Throughput: %12.0f events/sec%n", throughput);
        System.out.printf("  Publish:    %12.3f seconds%n", publishDuration);
        System.out.printf("  Process:    %12.3f seconds%n", processDuration);
        System.out.println("═".repeat(64));

        assertTrue(completed);
    }

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