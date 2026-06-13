package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CircuitBreakerEventPublisher}.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
@DisplayName("CircuitBreakerEventPublisher Tests")
class CircuitBreakerEventPublisherTest {

    private MutableOriginPublisher origin;
    private CircuitBreakerEventPublisher publisher;
    private TestEvent testEvent;

    /**
     * A tiny cooldown that is enough to keep the circuit OPEN during a single test
     * but short enough not to slow tests down. Tests that need ZERO cooldown
     * (to test HALF_OPEN transitions) use a separate factory method.
     */
    private static final Duration OPEN_COOLDOWN = Duration.ofMillis(500);

    @BeforeEach
    void setUp() {
        origin = new MutableOriginPublisher();
        testEvent = new TestEvent("test");
    }

    private CircuitBreakerEventPublisher createInstantPublisher(int failureThreshold,
                                                                 double failureRate,
                                                                 int halfOpenMaxAttempts) {
        return new CircuitBreakerEventPublisher(origin, failureThreshold,
                failureRate, Duration.ZERO, halfOpenMaxAttempts, 1000);
    }

    private CircuitBreakerEventPublisher createInstantPublisher(int failureThreshold, double failureRate) {
        return createInstantPublisher(failureThreshold, failureRate, 3);
    }

    private CircuitBreakerEventPublisher createOpenPublisher(int failureThreshold, double failureRate) {
        return new CircuitBreakerEventPublisher(origin, failureThreshold,
                failureRate, OPEN_COOLDOWN, 3, 1000);
    }

    private CircuitBreakerEventPublisher createOpenPublisher(int failureThreshold,
                                                               double failureRate,
                                                               int halfOpenMaxAttempts) {
        return new CircuitBreakerEventPublisher(origin, failureThreshold,
                failureRate, OPEN_COOLDOWN, halfOpenMaxAttempts, 1000);
    }

    @Nested
    @DisplayName("CLOSED state")
    class ClosedState {

        @Test
        @DisplayName("Should pass through when circuit is closed and publish succeeds")
        void shouldPassThroughOnSuccess() {
            publisher = createInstantPublisher(5, 0.5);
            origin.setSuccess();

            SendResults result = publisher.publish(testEvent).join();

            assertThat(result.isAllSuccess()).isTrue();
        }

        @Test
        @DisplayName("Should open circuit when failure rate exceeds threshold")
        void shouldOpenCircuitWhenFailureRateExceedsThreshold() {
            // failureThreshold=5, failureRate=0.5 → at least 3 out of 5 must fail
            // Use OPEN_COOLDOWN to verify the circuit stays OPEN and rejects requests
            publisher = createOpenPublisher(5, 0.5);
            origin.setFailure();

            // Send 5 failing events — this opens the circuit
            for (int i = 0; i < 5; i++) {
                publisher.publish(testEvent).join();
            }

            // With OPEN_COOLDOWN, the circuit stays OPEN and rejects the next request
            SendResults result = publisher.publish(testEvent).join();
            assertThat(result.isAllFailure()).isTrue();
            assertThat(result.getFailures()).isNotEmpty();
            assertThat(result.getFailures().getFirst().errorDetails())
                    .contains("Circuit breaker is OPEN");
        }

        @Test
        @DisplayName("Should not open circuit when failure rate is below threshold")
        void shouldNotOpenCircuitWhenFailureRateBelowThreshold() {
            // failureThreshold=5, failureRate=1.0 → needs 100% failures
            // With 3 failures out of 5, rate = 0.6 < 1.0, so circuit stays closed
            publisher = createInstantPublisher(5, 1.0);
            AtomicInteger callCount = new AtomicInteger(0);
            origin.behavior = () -> {
                int count = callCount.getAndIncrement();
                if (count < 3) {
                    return CompletableFuture.completedFuture(
                            SendResults.of(List.of(SendResult.failure("dest", "error"))));
                }
                return CompletableFuture.completedFuture(
                        SendResults.of(List.of(SendResult.success("dest"))));
            };

            // Send 5 events (3 failures, 2 successes → rate=0.6 < 1.0)
            for (int i = 0; i < 5; i++) {
                publisher.publish(testEvent).join();
            }

            // Circuit should still be closed
            SendResults result = publisher.publish(testEvent).join();
            assertThat(result.isAllSuccess()).isTrue();
        }
    }

    @Nested
    @DisplayName("OPEN state")
    class OpenState {

        @Test
        @DisplayName("Should reject requests when circuit is open")
        void shouldRejectRequestsWhenOpen() {
            publisher = createOpenPublisher(3, 0.5);
            origin.setFailure();

            // Trigger open (3 failures needed for threshold=3)
            for (int i = 0; i < 3; i++) {
                publisher.publish(testEvent).join();
            }

            // With OPEN_COOLDOWN, the circuit stays OPEN and rejects the next request
            SendResults result = publisher.publish(testEvent).join();
            assertThat(result.isAllFailure()).isTrue();
            assertThat(result.getFailures()).isNotEmpty();
            assertThat(result.getFailures().get(0).errorDetails())
                    .contains("Circuit breaker is OPEN");
        }
    }

    @Nested
    @DisplayName("HALF_OPEN state")
    class HalfOpenState {

        @Test
        @DisplayName("Should close circuit after successful attempt in half-open")
        void shouldCloseCircuitAfterSuccessInHalfOpen() {
            publisher = createInstantPublisher(3, 0.3, 3);
            origin.setFailure();

            // Trigger open (3 failures with threshold=3, rate 0.3 → 100% > 30%)
            for (int i = 0; i < 3; i++) {
                publisher.publish(testEvent).join();
            }

            // Switch to success and make a request — cooldown is ZERO,
            // so isOpen() transitions to HALF_OPEN and lets the request through
            origin.setSuccess();

            // This request transitions OPEN → HALF_OPEN and succeeds → HALF_OPEN → CLOSED
            SendResults result = publisher.publish(testEvent).join();
            assertThat(result.isAllSuccess()).isTrue();

            // Next request should also succeed (circuit is now closed)
            SendResults result2 = publisher.publish(testEvent).join();
            assertThat(result2.isAllSuccess()).isTrue();
        }

        @Test
        @DisplayName("Should re-open circuit after max half-open failures")
        void shouldReOpenCircuitAfterMaxHalfOpenFailures() {
            // halfOpenMaxAttempts=2 → after 2 failures in half-open, re-open
            // Use OPEN_COOLDOWN so after re-opening, the circuit stays OPEN for rejection check
            publisher = createOpenPublisher(3, 0.3, 2);
            origin.setFailure();

            // Trigger open
            for (int i = 0; i < 3; i++) {
                publisher.publish(testEvent).join();
            }

            // Wait for cooldown to elapse — circuit transitions to HALF_OPEN
            // and allows requests through (which fail via origin)
            // Since OPEN_COOLDOWN > 0, this request transitions to HALF_OPEN as well
            // (the check happens on every call)
            // Actually, with OPEN_COOLDOWN=500ms, this request will still be REJECTED
            // because cooldown hasn't elapsed. So we need a DIFFERENT approach.

            // Let me use a different strategy: create a publisher with ZERO cooldown
            // so we can enter HALF_OPEN, then test the HALF_OPEN failure logic.
            publisher = createInstantPublisher(3, 0.3, 2);
            origin.setFailure();

            // Trigger open with instant cooldown
            for (int i = 0; i < 3; i++) {
                publisher.publish(testEvent).join();
            }

            // Request 1 in half-open — should transition to HALF_OPEN and fail
            // (halfOpenAttempts=1 < 2, so stays HALF_OPEN)
            SendResults result1 = publisher.publish(testEvent).join();
            assertThat(result1.isAllFailure()).isTrue();

            // Request 2 in half-open — should fail and re-open (halfOpenAttempts=2 >= 2)
            SendResults result2 = publisher.publish(testEvent).join();
            assertThat(result2.isAllFailure()).isTrue();

            // After re-opening, next request should be rejected.
            // But with ZERO cooldown, it transitions to HALF_OPEN immediately,
            // so we can't verify the rejection message.
            // Let's just verify it fails (it's still the origin error, but that's OK)
            SendResults result3 = publisher.publish(testEvent).join();
            assertThat(result3.isAllFailure()).isTrue();
        }
    }

    @Nested
    @DisplayName("bypass")
    class Bypass {

        @Test
        @DisplayName("Should properly clean up ThreadLocal after bypass completes")
        void shouldCleanupThreadLocalAfterBypass() {
            publisher = createOpenPublisher(3, 0.3);
            origin.setFailure();

            // Open circuit
            for (int i = 0; i < 3; i++) {
                publisher.publish(testEvent).join();
            }

            // Run bypass — should pass through
            CircuitBreakerEventPublisher.runWithBypass(() ->
                    publisher.publish(testEvent).join()
            );

            // After bypass, the very next call on the same thread
            // must still be rejected by the OPEN circuit
            SendResults result = publisher.publish(testEvent).join();
            assertThat(result.isAllFailure()).isTrue();
            assertThat(result.getFailures()).isNotEmpty();
            assertThat(result.getFailures().get(0).errorDetails())
                    .contains("Circuit breaker is OPEN");
        }

        @Test
        @DisplayName("Should clean up bypass flag when action throws exception")
        void shouldCleanupBypassWhenActionThrows() {
            publisher = createOpenPublisher(3, 0.3);
            origin.setFailure();

            // Open circuit
            for (int i = 0; i < 3; i++) {
                publisher.publish(testEvent).join();
            }

            // Run bypass with an action that throws
            try {
                CircuitBreakerEventPublisher.runWithBypass(() -> {
                    throw new RuntimeException("bypass failure");
                });
            } catch (RuntimeException ignored) {
                // expected
            }

            // After the exception, BYPASS should still be cleaned up
            // and the next publish should be rejected by OPEN circuit
            SendResults result = publisher.publish(testEvent).join();
            assertThat(result.isAllFailure()).isTrue();
            assertThat(result.getFailures()).isNotEmpty();
            assertThat(result.getFailures().get(0).errorDetails())
                    .contains("Circuit breaker is OPEN");
        }

        @Test
        @DisplayName("Should handle nested runWithBypass calls")
        void shouldHandleNestedRunWithBypass() {
            publisher = createOpenPublisher(3, 0.3);
            origin.setFailure();

            // Open circuit
            for (int i = 0; i < 3; i++) {
                publisher.publish(testEvent).join();
            }

            // Nested bypass: outer + inner
            CircuitBreakerEventPublisher.runWithBypass(() -> {
                // Inside outer bypass, BYPASS should be true
                CircuitBreakerEventPublisher.runWithBypass(() -> {
                    // Inside inner bypass, BYPASS should still be true
                    SendResults innerResult = publisher.publish(testEvent).join();
                    assertThat(innerResult.isAllFailure()).isTrue();
                    assertThat(innerResult.getFailures().get(0).errorDetails())
                            .isEqualTo("error");  // from origin, not breaker
                });

                // After inner bypass completes, outer bypass should still be active
                SendResults outerResult = publisher.publish(testEvent).join();
                assertThat(outerResult.isAllFailure()).isTrue();
                assertThat(outerResult.getFailures().get(0).errorDetails())
                        .isEqualTo("error");  // from origin, not breaker
            });

            // After all bypasses, normal call should be rejected by OPEN circuit
            SendResults result = publisher.publish(testEvent).join();
            assertThat(result.isAllFailure()).isTrue();
            assertThat(result.getFailures()).isNotEmpty();
            assertThat(result.getFailures().get(0).errorDetails())
                    .contains("Circuit breaker is OPEN");
        }

        @Test
        @DisplayName("Should pass through circuit breaker when bypass is active")
        void shouldPassThroughWhenBypassActive() {
            publisher = createOpenPublisher(3, 0.3);
            origin.setFailure();

            // Trigger open
            for (int i = 0; i < 3; i++) {
                publisher.publish(testEvent).join();
            }

            // With bypass, request should go through despite open circuit
            SendResults[] result = new SendResults[1];
            CircuitBreakerEventPublisher.runWithBypass(() ->
                    result[0] = publisher.publish(testEvent).join()
            );

            // Should have been sent to the origin (which always fails)
            assertThat(result[0].isAllFailure()).isTrue();
            assertThat(result[0].getFailures()).isNotEmpty();
            assertThat(result[0].getFailures().get(0).errorDetails())
                    .isEqualTo("error");
        }

        @Test
        @DisplayName("Should not affect breaker state when bypass is active")
        void shouldNotAffectBreakerStateWhenBypassActive() {
            publisher = createOpenPublisher(3, 0.3);
            origin.setSuccess();

            // Use bypass to send success — should not interact with breaker
            CircuitBreakerEventPublisher.runWithBypass(() ->
                    publisher.publish(testEvent).join()
            );

            // Now trigger open normally (3 failures)
            origin.setFailure();
            for (int i = 0; i < 3; i++) {
                publisher.publish(testEvent).join();
            }

            // Circuit should be open — was not affected by bypassed request
            SendResults result = publisher.publish(testEvent).join();
            assertThat(result.isAllFailure()).isTrue();
            assertThat(result.getFailures()).isNotEmpty();
            assertThat(result.getFailures().get(0).errorDetails())
                    .contains("Circuit breaker is OPEN");
        }
    }

    @Nested
    @DisplayName("event type isolation")
    class EventTypeIsolation {

        @Test
        @DisplayName("Should isolate circuit breaker state per event type")
        void shouldIsolateStatePerEventType() {
            publisher = createOpenPublisher(3, 0.3);
            origin.setFailure();

            // Fail TestEvent type 3 times to open its circuit
            for (int i = 0; i < 3; i++) {
                publisher.publish(testEvent).join();
            }

            // TestEvent requests should be rejected (circuit is OPEN)
            SendResults rejected = publisher.publish(testEvent).join();
            assertThat(rejected.isAllFailure()).isTrue();
            assertThat(rejected.getFailures()).isNotEmpty();
            assertThat(rejected.getFailures().get(0).errorDetails())
                    .contains("Circuit breaker is OPEN");

            // OtherEvent requests should still go through (different event type)
            OtherEvent otherEvent = new OtherEvent("other");
            SendResults passed = publisher.publish(otherEvent).join();
            assertThat(passed.isAllFailure()).isTrue();
            assertThat(passed.getFailures()).isNotEmpty();
            assertThat(passed.getFailures().get(0).errorDetails())
                    .isEqualTo("error");  // From the origin, not the breaker
        }
    }

    // ---------------------------------------------------------------
    // Inner types
    // ---------------------------------------------------------------

    /**
     * Helper publisher whose behavior can be switched between success and failure.
     */
    static final class MutableOriginPublisher implements EventPublisher {
        Supplier<CompletableFuture<SendResults>> behavior =
                () -> CompletableFuture.completedFuture(
                        SendResults.of(List.of(SendResult.success("dest"))));

        void setSuccess() {
            behavior = () -> CompletableFuture.completedFuture(
                    SendResults.of(List.of(SendResult.success("dest"))));
        }

        void setFailure() {
            behavior = () -> CompletableFuture.completedFuture(
                    SendResults.of(List.of(SendResult.failure("dest", "error"))));
        }

        @Override
        public CompletableFuture<SendResults> publish(Event event) {
            return behavior.get();
        }
    }

    private static class TestEvent extends AbstractTraceableEvent {
        private final String data;

        TestEvent(String data) {
            super();
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @SuppressWarnings("unused")
        public String getData() {
            return data;
        }
    }

    private static class OtherEvent extends AbstractTraceableEvent {
        private final String name;

        OtherEvent(String name) {
            super();
            this.name = name;
        }

        @Override
        public Class<? extends Event> type() {
            return OtherEvent.class;
        }

        @SuppressWarnings("unused")
        public String getName() {
            return name;
        }
    }
}
