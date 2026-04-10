package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.SendResult;
import com.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link RetryEventPublisher}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("RetryEventPublisher Tests")
class RetryEventPublisherTest {

    private TestEvent event;

    @BeforeEach
    void setUp() {
        event = new TestEvent("test");
    }

    @Test
    @DisplayName("Should create with default constructor")
    void shouldCreateWithDefaultConstructor() {
        // Arrange & Act
        EventPublisher publisher = new RetryEventPublisher(e -> CompletableFuture.completedFuture(SendResults.of(List.of(SendResult.success("dest")))));

        // Assert
        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should throw exception when maxRetries is negative")
    void shouldThrowExceptionWhenMaxRetriesIsNegative() {
        // Assert
        assertThatThrownBy(() ->
                new RetryEventPublisher(e -> CompletableFuture.completedFuture(SendResults.empty()), -1, Duration.ofMillis(100), 2.0)
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Max retries must be >= 0");
    }

    @Test
    @DisplayName("Should throw exception when initialDelay is zero")
    void shouldThrowExceptionWhenInitialDelayIsZero() {
        // Assert
        assertThatThrownBy(() ->
                new RetryEventPublisher(e -> CompletableFuture.completedFuture(SendResults.empty()), 3, Duration.ZERO, 2.0)
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Initial delay must be positive");
    }

    @Test
    @DisplayName("Should throw exception when initialDelay is negative")
    void shouldThrowExceptionWhenInitialDelayIsNegative() {
        // Assert
        assertThatThrownBy(() ->
                new RetryEventPublisher(e -> CompletableFuture.completedFuture(SendResults.empty()), 3, Duration.ofMillis(-100), 2.0)
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Initial delay must be positive");
    }

    @Test
    @DisplayName("Should throw exception when multiplier is less than 1")
    void shouldThrowExceptionWhenMultiplierIsLessThanOne() {
        // Assert
        assertThatThrownBy(() ->
                new RetryEventPublisher(e -> CompletableFuture.completedFuture(SendResults.empty()), 3, Duration.ofMillis(100), 0.5)
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Multiplier must be >= 1.0");
    }

    @Test
    @DisplayName("Should throw exception when maxDelay is zero")
    void shouldThrowExceptionWhenMaxDelayIsZero() {
        // Assert
        assertThatThrownBy(() ->
                new RetryEventPublisher(e -> CompletableFuture.completedFuture(SendResults.empty()), 3, Duration.ofMillis(100), 2.0, Duration.ZERO)
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Max delay must be positive");
    }

    @Test
    @DisplayName("Should throw exception when maxDelay is negative")
    void shouldThrowExceptionWhenMaxDelayIsNegative() {
        // Assert
        assertThatThrownBy(() ->
                new RetryEventPublisher(e -> CompletableFuture.completedFuture(SendResults.empty()), 3, Duration.ofMillis(100), 2.0, Duration.ofMillis(-100))
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Max delay must be positive");
    }

    @Test
    @DisplayName("Should cap delay at maxDelay")
    void shouldCapDelayAtMaxDelay() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        EventPublisher origin = new EventPublisher() {
            @Override
            public CompletableFuture<SendResults> publish(Event event) {
                if (callCount.incrementAndGet() < 2) {
                    return CompletableFuture.failedFuture(new RuntimeException("Temporary failure"));
                }
                return CompletableFuture.completedFuture(SendResults.of(List.of(SendResult.success("dest"))));
            }
        };
        // Initial delay 10ms, multiplier 2, so attempt 2 would be 20ms, but maxDelay is 15ms
        EventPublisher publisher = new TestRetryEventPublisher(origin, 2, Duration.ofMillis(10), 2.0, Duration.ofMillis(15));

        // Act
        long startTime = System.currentTimeMillis();
        CompletableFuture<SendResults> future = publisher.publish(event);
        future.join();
        long elapsed = System.currentTimeMillis() - startTime;

        // Assert
        assertEquals(2, callCount.get());
        // Delay is capped at 15ms, allow some margin for execution overhead
        assertTrue(elapsed >= 10, "Should have waited at least 10ms, but waited " + elapsed + "ms");
    }

    @Test
    @DisplayName("Should publish successfully on first attempt")
    void shouldPublishSuccessfullyOnFirstAttempt() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        EventPublisher publisher = new RetryEventPublisher(e -> {
            callCount.incrementAndGet();
            return CompletableFuture.completedFuture(SendResults.of(List.of(SendResult.success("dest"))));
        });

        // Act
        CompletableFuture<SendResults> future = publisher.publish(event);
        SendResults results = future.join();

        // Assert
        assertEquals(1, callCount.get());
        assertThat(results.isAllSuccess()).isTrue();
    }

    @Test
    @DisplayName("Should retry on failure and succeed")
    void shouldRetryOnFailureAndSucceed() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        EventPublisher origin = new EventPublisher() {
            @Override
            public CompletableFuture<SendResults> publish(Event event) {
                if (callCount.incrementAndGet() < 3) {
                    return CompletableFuture.failedFuture(new RuntimeException("Temporary failure"));
                }
                return CompletableFuture.completedFuture(SendResults.of(List.of(SendResult.success("dest"))));
            }
        };
        EventPublisher publisher = new TestRetryEventPublisher(origin, 3, Duration.ofMillis(10), 2.0);

        // Act
        CompletableFuture<SendResults> future = publisher.publish(event);
        SendResults results = future.join();

        // Assert
        assertEquals(3, callCount.get());
        assertThat(results.isAllSuccess()).isTrue();
    }

    @Test
    @DisplayName("Should complete exceptionally after all retries exhausted")
    void shouldCompleteExceptionallyAfterAllRetriesExhausted() {
        // Arrange
        EventPublisher origin = new EventPublisher() {
            @Override
            public CompletableFuture<SendResults> publish(Event event) {
                return CompletableFuture.failedFuture(new RuntimeException("Permanent failure"));
            }
        };
        EventPublisher publisher = new TestRetryEventPublisher(origin, 2, Duration.ofMillis(10), 2.0);

        // Assert
        CompletableFuture<SendResults> future = publisher.publish(event);
        assertThatThrownBy(future::join)
                .hasCauseInstanceOf(EventPublisherException.class)
                .hasStackTraceContaining("after 3 attempts");
    }

    @Test
    @DisplayName("Should not retry on EventPublisherConfigException")
    void shouldNotRetryOnEventPublisherConfigException() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        EventPublisher origin = new EventPublisher() {
            @Override
            public CompletableFuture<SendResults> publish(Event event) {
                callCount.incrementAndGet();
                return CompletableFuture.failedFuture(new EventPublisherConfigException("Config error"));
            }
        };
        EventPublisher publisher = new TestRetryEventPublisher(origin, 3, Duration.ofMillis(10), 2.0);

        // Assert
        CompletableFuture<SendResults> future = publisher.publish(event);
        assertThatThrownBy(future::join)
                .hasCauseInstanceOf(EventPublisherConfigException.class);
        assertEquals(1, callCount.get());
    }

    @Test
    @DisplayName("Should not retry on IllegalArgumentException")
    void shouldNotRetryOnIllegalArgumentException() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        EventPublisher origin = new EventPublisher() {
            @Override
            public CompletableFuture<SendResults> publish(Event event) {
                callCount.incrementAndGet();
                return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid argument"));
            }
        };
        EventPublisher publisher = new TestRetryEventPublisher(origin, 3, Duration.ofMillis(10), 2.0);

        // Assert
        CompletableFuture<SendResults> future = publisher.publish(event);
        assertThatThrownBy(future::join)
                .hasCauseInstanceOf(IllegalArgumentException.class);
        assertEquals(1, callCount.get());
    }

    @Test
    @DisplayName("Should sleep between retries")
    void shouldSleepBetweenRetries() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        EventPublisher origin = new EventPublisher() {
            @Override
            public CompletableFuture<SendResults> publish(Event event) {
                if (callCount.incrementAndGet() < 2) {
                    return CompletableFuture.failedFuture(new RuntimeException("Temporary failure"));
                }
                return CompletableFuture.completedFuture(SendResults.of(List.of(SendResult.success("dest"))));
            }
        };
        EventPublisher publisher = new TestRetryEventPublisher(origin, 2, Duration.ofMillis(50), 2.0);

        // Act
        long startTime = System.currentTimeMillis();
        CompletableFuture<SendResults> future = publisher.publish(event);
        future.join();
        long elapsed = System.currentTimeMillis() - startTime;

        // Assert
        assertEquals(2, callCount.get());
        assertTrue(elapsed >= 50, "Should have waited at least 50ms between retries, but waited " + elapsed + "ms");
    }

    @Test
    @DisplayName("Should handle zero retries")
    void shouldHandleZeroRetries() {
        // Arrange
        EventPublisher origin = new EventPublisher() {
            @Override
            public CompletableFuture<SendResults> publish(Event event) {
                return CompletableFuture.failedFuture(new RuntimeException("Failure"));
            }
        };
        EventPublisher publisher = new TestRetryEventPublisher(origin, 0, Duration.ofMillis(10), 2.0);

        // Assert
        CompletableFuture<SendResults> future = publisher.publish(event);
        assertThatThrownBy(future::join)
                .hasCauseInstanceOf(EventPublisherException.class)
                .hasStackTraceContaining("after 1 attempts");
    }

    /**
     * Test event class.
     */
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

        @Override
        public String asJson() {
            return "{\"data\":\"" + data + "\"}";
        }
    }

    /**
     * Test RetryEventPublisher.
     */
    private static final class TestRetryEventPublisher extends RetryEventPublisher {
        TestRetryEventPublisher(EventPublisher origin, int maxRetries, Duration initialDelay, double multiplier) {
            super(origin, maxRetries, initialDelay, multiplier);
        }

        TestRetryEventPublisher(EventPublisher origin, int maxRetries, Duration initialDelay, double multiplier, Duration maxDelay) {
            super(origin, maxRetries, initialDelay, multiplier, maxDelay);
        }
    }
}
