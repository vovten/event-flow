package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.registry.EventHandlerRegistry;
import io.github.vovten.eventflow.retry.FailureKind;
import io.github.vovten.eventflow.retry.ResolvedRetryPolicy;
import io.github.vovten.eventflow.retry.RetryDelayScheduler;
import io.github.vovten.eventflow.retry.RetryMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("UnifiedEventDispatcher Local Retry Tests")
class UnifiedEventDispatcherRetryTest {

    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    @DisplayName("Should absorb transient failures locally")
    void shouldAbsorbTransientFailuresLocally() {
        RecordingDelayScheduler scheduler = new RecordingDelayScheduler();
        PolicyHandler handler = new PolicyHandler(
                "handler", policy(3, 3, Set.of(TimeoutException.class), Set.of()),
                2, new RuntimeException(new TimeoutException("slow")));

        HandlerResults results = dispatcher(handler, scheduler).dispatch(new TestEvent()).join();

        assertThat(results.isAllSuccess()).isTrue();
        assertThat(handler.invocations()).isEqualTo(3);
        assertThat(scheduler.delays()).containsExactly(Duration.ofMillis(10), Duration.ofMillis(20));
    }

    @Test
    @DisplayName("Should hand off to durable retry when the local budget is exhausted")
    void shouldHandOffToDurableRetry() {
        RecordingDelayScheduler scheduler = new RecordingDelayScheduler();
        PolicyHandler handler = new PolicyHandler(
                "handler", policy(5, 2, Set.of(TimeoutException.class), Set.of()),
                Integer.MAX_VALUE, new RuntimeException(new TimeoutException("slow")));

        HandlerResults results = dispatcher(handler, scheduler).dispatch(new TestEvent()).join();
        HandlerResult failure = results.getFirstFailure().orElseThrow();

        assertThat(failure.failureKind()).isEqualTo(FailureKind.TRANSIENT_EXHAUSTED);
        assertThat(failure.retryable()).isTrue();
        assertThat(failure.attempts()).isEqualTo(3);
        assertThat(failure.retryAfterMs()).isEqualTo(40L);
        assertThat(failure.policyName()).isEqualTo("test-policy");
        assertThat(handler.invocations()).isEqualTo(3);
        assertThat(scheduler.delays()).containsExactly(Duration.ofMillis(10), Duration.ofMillis(20));
    }

    @Test
    @DisplayName("Should not retry locally when localMaxRetries is zero")
    void shouldNotRetryLocallyWhenLocalMaxRetriesIsZero() {
        RecordingDelayScheduler scheduler = new RecordingDelayScheduler();
        PolicyHandler handler = new PolicyHandler(
                "handler", policy(3, 0, Set.of(TimeoutException.class), Set.of()),
                Integer.MAX_VALUE, new RuntimeException(new TimeoutException("slow")));

        HandlerResults results = dispatcher(handler, scheduler).dispatch(new TestEvent()).join();
        HandlerResult failure = results.getFirstFailure().orElseThrow();

        assertThat(failure.retryable()).isTrue();
        assertThat(failure.attempts()).isEqualTo(1);
        assertThat(failure.retryAfterMs()).isEqualTo(10L);
        assertThat(handler.invocations()).isEqualTo(1);
        assertThat(scheduler.delays()).isEmpty();
    }

    @Test
    @DisplayName("Should not retry a permanent failure")
    void shouldNotRetryPermanentFailure() {
        RecordingDelayScheduler scheduler = new RecordingDelayScheduler();
        PolicyHandler handler = new PolicyHandler(
                "handler", policy(3, 3, Set.of(TimeoutException.class), Set.of()),
                Integer.MAX_VALUE, new IllegalArgumentException("bad"));

        HandlerResults results = dispatcher(handler, scheduler).dispatch(new TestEvent()).join();
        HandlerResult failure = results.getFirstFailure().orElseThrow();

        assertThat(failure.failureKind()).isEqualTo(FailureKind.PERMANENT);
        assertThat(failure.retryable()).isFalse();
        assertThat(failure.attempts()).isEqualTo(1);
        assertThat(handler.invocations()).isEqualTo(1);
        assertThat(scheduler.delays()).isEmpty();
    }

    @Test
    @DisplayName("Should stop when the total attempt budget is exhausted")
    void shouldStopWhenTotalBudgetExhausted() {
        RecordingDelayScheduler scheduler = new RecordingDelayScheduler();
        PolicyHandler handler = new PolicyHandler(
                "handler", policy(3, 0, Set.of(TimeoutException.class), Set.of()),
                Integer.MAX_VALUE, new RuntimeException(new TimeoutException("slow")));
        Envelope<TestEvent> envelope = Envelope.of(new TestEvent())
                .withAdditionalMetadata(RetryMetadata.ATTEMPT, "4");

        HandlerResults results = dispatcher(handler, scheduler).dispatch(envelope).join();
        HandlerResult failure = results.getFirstFailure().orElseThrow();

        assertThat(failure.retryable()).isFalse();
        assertThat(failure.attempts()).isEqualTo(4);
        assertThat(handler.invocations()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should honour the attempt number from envelope metadata")
    void shouldHonourAttemptFromMetadata() {
        RecordingDelayScheduler scheduler = new RecordingDelayScheduler();
        PolicyHandler handler = new PolicyHandler(
                "handler", policy(5, 2, Set.of(TimeoutException.class), Set.of()),
                Integer.MAX_VALUE, new RuntimeException(new TimeoutException("slow")));
        Envelope<TestEvent> envelope = Envelope.of(new TestEvent())
                .withAdditionalMetadata(RetryMetadata.ATTEMPT, "2");

        HandlerResults results = dispatcher(handler, scheduler).dispatch(envelope).join();
        HandlerResult failure = results.getFirstFailure().orElseThrow();

        assertThat(failure.attempts()).isEqualTo(3);
        assertThat(failure.retryAfterMs()).isEqualTo(40L);
        assertThat(handler.invocations()).isEqualTo(2);
        assertThat(scheduler.delays()).containsExactly(Duration.ofMillis(20));
    }

    @Test
    @DisplayName("Should estimate the maximum local processing time")
    void shouldEstimateMaxProcessingMs() {
        RecordingDelayScheduler scheduler = new RecordingDelayScheduler();
        PolicyHandler handler = new PolicyHandler(
                "handler", policy(5, 2, Set.of(TimeoutException.class), Set.of()),
                Integer.MAX_VALUE, new RuntimeException(new TimeoutException("slow")));
        UnifiedEventDispatcher dispatcher = dispatcher(handler, scheduler);

        assertThat(dispatcher.maxProcessingMs(new TestEvent())).isEqualTo(30L);
    }

    @Test
    @DisplayName("Should isolate retries per handler")
    void shouldIsolateRetriesPerHandler() {
        RecordingDelayScheduler scheduler = new RecordingDelayScheduler();
        PolicyHandler flaky = new PolicyHandler(
                "flaky", policy(3, 3, Set.of(TimeoutException.class), Set.of()),
                1, new RuntimeException(new TimeoutException("slow")));
        PolicyHandler healthy = new PolicyHandler(
                "healthy", policy(3, 3, Set.of(), Set.of()), 0, null);

        Map<Class<?>, List<EventHandler>> handlerMap = Map.of(
                TestEvent.class, List.of(flaky, healthy));
        EventHandlerRegistry registry = new MapBasedHandlerRegistry(handlerMap);
        UnifiedEventDispatcher dispatcher = new UnifiedEventDispatcher(
                executor, registry, List.of(), null, scheduler, 10);

        HandlerResults results = dispatcher.dispatch(new TestEvent()).join();

        assertThat(results.isAllSuccess()).isTrue();
        assertThat(flaky.invocations()).isEqualTo(2);
        assertThat(healthy.invocations()).isEqualTo(1);
    }

    private UnifiedEventDispatcher dispatcher(PolicyHandler handler, RecordingDelayScheduler scheduler) {
        Map<Class<?>, List<EventHandler>> handlerMap = Map.of(
                TestEvent.class, List.of(handler));
        EventHandlerRegistry registry = new MapBasedHandlerRegistry(handlerMap);
        return new UnifiedEventDispatcher(executor, registry, List.of(), null, scheduler, 10);
    }

    private static ResolvedRetryPolicy policy(int maxRetries, int localMaxRetries,
                                              Set<Class<? extends Throwable>> retryOn,
                                              Set<Class<? extends Throwable>> noRetryOn) {
        return new ResolvedRetryPolicy("test-policy", retryOn, noRetryOn, maxRetries, localMaxRetries,
                Duration.ofMillis(10), 2.0, Duration.ofSeconds(1), 0.0);
    }

    private static final class TestEvent extends AbstractTraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }

    private static final class PolicyHandler implements EventHandler {

        private final String name;
        private final ResolvedRetryPolicy policy;
        private final int failuresBeforeSuccess;
        private final RuntimeException error;
        private final AtomicInteger invocations = new AtomicInteger();

        private PolicyHandler(String name, ResolvedRetryPolicy policy,
                              int failuresBeforeSuccess, RuntimeException error) {
            this.name = name;
            this.policy = policy;
            this.failuresBeforeSuccess = failuresBeforeSuccess;
            this.error = error;
        }

        @Override
        public void onEvent(Event event) {
            if (invocations.incrementAndGet() <= failuresBeforeSuccess) {
                throw error;
            }
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public ResolvedRetryPolicy resolvedRetryPolicy() {
            return policy;
        }

        private int invocations() {
            return invocations.get();
        }
    }

    private static final class RecordingDelayScheduler implements RetryDelayScheduler {

        private final List<Duration> delays = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<Void> delay(Duration delay) {
            delays.add(delay);
            return CompletableFuture.completedFuture(null);
        }

        private List<Duration> delays() {
            return delays;
        }
    }
}
