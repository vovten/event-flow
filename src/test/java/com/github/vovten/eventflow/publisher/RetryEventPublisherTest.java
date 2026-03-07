package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RetryEventPublisher.
 */
class RetryEventPublisherTest {

    private EventPublisher delegate;

    @BeforeEach
    void setUp() {
        delegate = mock(EventPublisher.class);
    }

    @Test
    void testConstructor_DefaultValues() {
        RetryEventPublisher publisher = new RetryEventPublisher(delegate);

        assertNotNull(publisher);
    }

    @Test
    void testConstructor_CustomValues() {
        RetryEventPublisher publisher = new RetryEventPublisher(delegate, 5, Duration.ofMillis(50), 1.5);

        assertNotNull(publisher);
    }

    @Test
    void testConstructor_InvalidMaxRetries() {
        assertThrows(IllegalArgumentException.class, () ->
                new RetryEventPublisher(delegate, -1, Duration.ofMillis(100), 2.0));
    }

    @Test
    void testConstructor_InvalidMultiplier() {
        assertThrows(IllegalArgumentException.class, () ->
                new RetryEventPublisher(delegate, 3, Duration.ofMillis(100), 0.5));
    }

    @Test
    void testPublish_Success_NoRetries() {
        RetryEventPublisher retryPublisher = new RetryEventPublisher(delegate);
        TestEvent event = new TestEvent();

        retryPublisher.publish(event);

        verify(delegate, times(1)).publish(event);
    }

    @Test
    void testPublish_FailureWithRetries() {
        doThrow(new EventPublisherException("Failed"))
                .doThrow(new EventPublisherException("Failed"))
                .doNothing()
                .when(delegate).publish(any());

        RetryEventPublisher retryPublisher = new RetryEventPublisher(delegate, 3, Duration.ofMillis(10), 2.0);
        TestEvent event = new TestEvent();

        retryPublisher.publish(event);

        verify(delegate, times(3)).publish(event);
    }

    @Test
    void testPublish_AllRetriesFail_ThrowsException() {
        doThrow(new EventPublisherException("Failed"))
                .when(delegate).publish(any());

        RetryEventPublisher retryPublisher = new RetryEventPublisher(delegate, 3, Duration.ofMillis(10), 2.0);
        TestEvent event = new TestEvent();

        assertThrows(EventPublisherException.class, () -> retryPublisher.publish(event));
        verify(delegate, times(4)).publish(event);
    }

    @Test
    void testPublish_RetryCount() {
        AtomicInteger callCount = new AtomicInteger(0);
        EventPublisher failingDelegate = e -> {
            callCount.incrementAndGet();
            throw new EventPublisherException("Failed");
        };
        RetryEventPublisher retryPublisher = new RetryEventPublisher(failingDelegate, 5, Duration.ofMillis(1), 2.0);

        TestEvent event = new TestEvent();
        assertThrows(EventPublisherException.class, () -> retryPublisher.publish(event));

        assertEquals(6, callCount.get());
    }

    @Test
    void testPublish_ConfigException_NoRetry() {
        doThrow(new EventPublisherConfigException("Config error"))
                .when(delegate).publish(any());

        RetryEventPublisher retryPublisher = new RetryEventPublisher(delegate, 3, Duration.ofMillis(10), 2.0);
        TestEvent event = new TestEvent();

        assertThrows(EventPublisherConfigException.class, () -> retryPublisher.publish(event));
        verify(delegate, times(1)).publish(event);
    }

    @Test
    void testPublish_IllegalArgumentException_NoRetry() {
        doThrow(new IllegalArgumentException("Invalid argument"))
                .when(delegate).publish(any());

        RetryEventPublisher retryPublisher = new RetryEventPublisher(delegate, 3, Duration.ofMillis(10), 2.0);
        TestEvent event = new TestEvent();

        assertThrows(IllegalArgumentException.class, () -> retryPublisher.publish(event));
        verify(delegate, times(1)).publish(event);
    }

    static class TestEvent implements Event {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }
}
