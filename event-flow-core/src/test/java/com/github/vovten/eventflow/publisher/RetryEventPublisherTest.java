package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

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
        EventPublisher publisher = new RetryEventPublisher(e -> {});

        // Assert
        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should throw exception when maxRetries is negative")
    void shouldThrowExceptionWhenMaxRetriesIsNegative() {
        // Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                new RetryEventPublisher(e -> {}, -1, Duration.ofMillis(100), 2.0)
        );
        assertEquals("Max retries must be >= 0", exception.getMessage());
    }

    @Test
    @DisplayName("Should throw exception when initialDelay is zero")
    void shouldThrowExceptionWhenInitialDelayIsZero() {
        // Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                new RetryEventPublisher(e -> {}, 3, Duration.ZERO, 2.0)
        );
        assertEquals("Initial delay must be positive", exception.getMessage());
    }

    @Test
    @DisplayName("Should throw exception when initialDelay is negative")
    void shouldThrowExceptionWhenInitialDelayIsNegative() {
        // Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                new RetryEventPublisher(e -> {}, 3, Duration.ofMillis(-100), 2.0)
        );
        assertEquals("Initial delay must be positive", exception.getMessage());
    }

    @Test
    @DisplayName("Should throw exception when multiplier is less than 1")
    void shouldThrowExceptionWhenMultiplierIsLessThanOne() {
        // Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                new RetryEventPublisher(e -> {}, 3, Duration.ofMillis(100), 0.5)
        );
        assertEquals("Multiplier must be >= 1.0", exception.getMessage());
    }

    @Test
    @DisplayName("Should publish successfully on first attempt")
    void shouldPublishSuccessfullyOnFirstAttempt() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        EventPublisher publisher = new RetryEventPublisher(e -> callCount.incrementAndGet());

        // Act
        publisher.publish(event);

        // Assert
        assertEquals(1, callCount.get());
    }

    @Test
    @DisplayName("Should retry on failure and succeed")
    void shouldRetryOnFailureAndSucceed() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        EventPublisher publisher = new TestRetryEventPublisher(e -> {
            if (callCount.incrementAndGet() < 3) {
                throw new RuntimeException("Temporary failure");
            }
        }, 3, Duration.ofMillis(10), 2.0);

        // Act
        publisher.publish(event);

        // Assert
        assertEquals(3, callCount.get());
    }

    @Test
    @DisplayName("Should throw exception after all retries exhausted")
    void shouldThrowExceptionAfterAllRetriesExhausted() {
        // Arrange
        EventPublisher publisher = new TestRetryEventPublisher(e -> {
            throw new RuntimeException("Permanent failure");
        }, 2, Duration.ofMillis(10), 2.0);

        // Assert
        EventPublisherException exception = assertThrows(EventPublisherException.class, () ->
                publisher.publish(event)
        );
        assertTrue(exception.getMessage().contains("after 3 attempts"));
    }

    @Test
    @DisplayName("Should not retry on EventPublisherConfigException")
    void shouldNotRetryOnEventPublisherConfigException() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        EventPublisher publisher = new TestRetryEventPublisher(e -> {
            callCount.incrementAndGet();
            throw new EventPublisherConfigException("Config error");
        }, 3, Duration.ofMillis(10), 2.0);

        // Assert
        assertThrows(EventPublisherConfigException.class, () -> publisher.publish(event));
        assertEquals(1, callCount.get());
    }

    @Test
    @DisplayName("Should not retry on IllegalArgumentException")
    void shouldNotRetryOnIllegalArgumentException() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        EventPublisher publisher = new TestRetryEventPublisher(e -> {
            callCount.incrementAndGet();
            throw new IllegalArgumentException("Invalid argument");
        }, 3, Duration.ofMillis(10), 2.0);

        // Assert
        assertThrows(IllegalArgumentException.class, () -> publisher.publish(event));
        assertEquals(1, callCount.get());
    }

    @Test
    @DisplayName("Should sleep between retries")
    void shouldSleepBetweenRetries() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        TestRetryEventPublisher publisher = new TestRetryEventPublisher(e -> {
            if (callCount.incrementAndGet() < 2) {
                throw new RuntimeException("Temporary failure");
            }
        }, 2, Duration.ofMillis(10), 2.0);

        // Act
        publisher.publish(event);

        // Assert
        assertTrue(publisher.lastSleepTime >= 10);
        assertEquals(2, callCount.get());
    }

    @Test
    @DisplayName("Should handle zero retries")
    void shouldHandleZeroRetries() {
        // Arrange
        EventPublisher publisher = new TestRetryEventPublisher(e -> {
            throw new RuntimeException("Failure");
        }, 0, Duration.ofMillis(10), 2.0);

        // Assert
        EventPublisherException exception = assertThrows(EventPublisherException.class, () ->
                publisher.publish(event)
        );
        assertTrue(exception.getMessage().contains("after 1 attempts"));
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
     * Test RetryEventPublisher that exposes sleep time.
     */
    private static final class TestRetryEventPublisher extends RetryEventPublisher {
        long lastSleepTime = 0;

        TestRetryEventPublisher(EventPublisher origin, int maxRetries, Duration initialDelay, double multiplier) {
            super(origin, maxRetries, initialDelay, multiplier);
        }

        @Override
        void sleep(long millis) {
            lastSleepTime = millis;
            // Don't actually sleep in tests
        }
    }
}
