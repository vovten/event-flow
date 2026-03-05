package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RetryEventPublisher
 */
class RetryEventPublisherTest {

    @Test
    @DisplayName("Should publish event successfully on first attempt")
    void shouldPublishEventSuccessfullyOnFirstAttempt() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        RetryEventPublisher retryPublisher = new RetryEventPublisher(
                mockDelegate, 3, Duration.ofMillis(10), 2.0
        );
        Event event = TestEvent.create("test");

        // when
        retryPublisher.publish(event);

        // then
        verify(mockDelegate, times(1)).publish(event);
    }

    @Test
    @DisplayName("Should retry on transient failure and succeed")
    void shouldRetryOnTransientFailureAndSucceed() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new RuntimeException("Temporary network error"))
                .doThrow(new RuntimeException("Temporary network error"))
                .doNothing()
                .when(mockDelegate).publish(any(Event.class));
        
        RetryEventPublisher retryPublisher = new RetryEventPublisher(
                mockDelegate, 3, Duration.ofMillis(10), 2.0
        );
        Event event = TestEvent.create("test");

        // when
        retryPublisher.publish(event);

        // then
        verify(mockDelegate, times(3)).publish(event);
    }

    @Test
    @DisplayName("Should throw exception after max retries exhausted")
    void shouldThrowExceptionAfterMaxRetriesExhausted() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new RuntimeException("Persistent error"))
                .when(mockDelegate).publish(any(Event.class));
        
        RetryEventPublisher retryPublisher = new RetryEventPublisher(
                mockDelegate, 2, Duration.ofMillis(10), 2.0
        );
        Event event = TestEvent.create("test");

        // when & then
        EventPublisherException exception = assertThrows(
                EventPublisherException.class,
                () -> retryPublisher.publish(event)
        );
        
        assertTrue(exception.getMessage().contains("after 3 attempts"));
        verify(mockDelegate, times(3)).publish(event);
    }

    @Test
    @DisplayName("Should not retry on EventPublisherConfigException")
    void shouldNotRetryOnEventPublisherConfigException() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new EventPublisherConfigException("Configuration error"))
                .when(mockDelegate).publish(any(Event.class));
        
        RetryEventPublisher retryPublisher = new RetryEventPublisher(
                mockDelegate, 3, Duration.ofMillis(10), 2.0
        );
        Event event = TestEvent.create("test");

        // when & then
        assertThrows(
                EventPublisherConfigException.class,
                () -> retryPublisher.publish(event)
        );
        
        verify(mockDelegate, times(1)).publish(event);
    }

    @Test
    @DisplayName("Should not retry on IllegalArgumentException")
    void shouldNotRetryOnIllegalArgumentException() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new IllegalArgumentException("Invalid argument"))
                .when(mockDelegate).publish(any(Event.class));
        
        RetryEventPublisher retryPublisher = new RetryEventPublisher(
                mockDelegate, 3, Duration.ofMillis(10), 2.0
        );
        Event event = TestEvent.create("test");

        // when & then
        assertThrows(
                IllegalArgumentException.class,
                () -> retryPublisher.publish(event)
        );
        
        verify(mockDelegate, times(1)).publish(event);
    }

    @Test
    @DisplayName("Should use exponential backoff delay")
    void shouldUseExponentialBackoffDelay() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new RuntimeException("Error"))
                .when(mockDelegate).publish(any(Event.class));
        
        RetryEventPublisher retryPublisher = spy(new RetryEventPublisher(
                mockDelegate, 3, Duration.ofMillis(100), 2.0
        ));
        Event event = TestEvent.create("test");

        // when
        assertThrows(EventPublisherException.class, () -> retryPublisher.publish(event));

        // then - verify delays: 100ms, 200ms, 400ms
        InOrder inOrder = inOrder(retryPublisher);
        inOrder.verify(retryPublisher).sleep(100L);
        inOrder.verify(retryPublisher).sleep(200L);
        inOrder.verify(retryPublisher).sleep(400L);
    }

    @Test
    @DisplayName("Should cap delay at 10 seconds")
    void shouldCapDelayAt10Seconds() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new RuntimeException("Error"))
                .when(mockDelegate).publish(any(Event.class));
        
        // With multiplier 3.0: 1000ms → 3000ms → 9000ms → 27000ms (capped at 10000ms)
        RetryEventPublisher retryPublisher = spy(new RetryEventPublisher(
                mockDelegate, 4, Duration.ofMillis(1000), 3.0
        ));
        Event event = TestEvent.create("test");

        // when
        assertThrows(EventPublisherException.class, () -> retryPublisher.publish(event));

        // then - last delay should be capped at 10000ms
        InOrder inOrder = inOrder(retryPublisher);
        inOrder.verify(retryPublisher).sleep(1000L);
        inOrder.verify(retryPublisher).sleep(3000L);
        inOrder.verify(retryPublisher).sleep(9000L);
        inOrder.verify(retryPublisher).sleep(10000L); // capped
    }

    @Test
    @DisplayName("Should use default constructor settings")
    void shouldUseDefaultConstructorSettings() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new RuntimeException("Error"))
                .when(mockDelegate).publish(any(Event.class));
        
        RetryEventPublisher retryPublisher = spy(new RetryEventPublisher(mockDelegate));
        Event event = TestEvent.create("test");

        // when
        assertThrows(EventPublisherException.class, () -> retryPublisher.publish(event));

        // then - default: 3 retries, 100ms initial delay, 2.0 multiplier
        InOrder inOrder = inOrder(retryPublisher);
        inOrder.verify(retryPublisher).sleep(100L);
        inOrder.verify(retryPublisher).sleep(200L);
        inOrder.verify(retryPublisher).sleep(400L);
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException for negative maxRetries")
    void shouldThrowIllegalArgumentExceptionForNegativeMaxRetries() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);

        // when & then
        assertThrows(
                IllegalArgumentException.class,
                () -> new RetryEventPublisher(mockDelegate, -1, Duration.ofMillis(100), 2.0)
        );
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException for zero initialDelay")
    void shouldThrowIllegalArgumentExceptionForZeroInitialDelay() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);

        // when & then
        assertThrows(
                IllegalArgumentException.class,
                () -> new RetryEventPublisher(mockDelegate, 3, Duration.ZERO, 2.0)
        );
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException for multiplier less than 1.0")
    void shouldThrowIllegalArgumentExceptionForMultiplierLessThan1() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);

        // when & then
        assertThrows(
                IllegalArgumentException.class,
                () -> new RetryEventPublisher(mockDelegate, 3, Duration.ofMillis(100), 0.5)
        );
    }
}
