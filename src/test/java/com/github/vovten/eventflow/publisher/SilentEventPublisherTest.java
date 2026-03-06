package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.test.TestEvent;
import com.github.vovten.eventflow.transport.OutgoingEventTransportException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SilentEventPublisher
 */
class SilentEventPublisherTest {

    @Test
    @DisplayName("Should publish event successfully without errors")
    void shouldPublishEventSuccessfullyWithoutErrors() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        SilentEventPublisher silentPublisher = new SilentEventPublisher(mockDelegate);
        Event event = TestEvent.create("test");

        // when
        assertDoesNotThrow(() -> silentPublisher.publish(event));

        // then
        verify(mockDelegate, times(1)).publish(event);
    }

    @Test
    @DisplayName("Should catch and log exception from delegate publisher")
    void shouldCatchAndLogExceptionFromDelegatePublisher() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new RuntimeException("Publish failed"))
                .when(mockDelegate).publish(any(Event.class));
        
        SilentEventPublisher silentPublisher = new SilentEventPublisher(mockDelegate);
        Event event = TestEvent.create("test");

        // when & then - should NOT throw
        assertDoesNotThrow(() -> silentPublisher.publish(event));
        
        verify(mockDelegate, times(1)).publish(event);
    }

    @Test
    @DisplayName("Should catch EventPublisherConfigException and log")
    void shouldCatchEventPublisherConfigExceptionAndLog() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new EventPublisherConfigException("Configuration error"))
                .when(mockDelegate).publish(any(Event.class));
        
        SilentEventPublisher silentPublisher = new SilentEventPublisher(mockDelegate);
        Event event = TestEvent.create("test");

        // when & then - should NOT throw
        assertDoesNotThrow(() -> silentPublisher.publish(event));
        
        verify(mockDelegate, times(1)).publish(event);
    }

    @Test
    @DisplayName("Should catch EventTransportException and log")
    void shouldCatchEventTransportExceptionAndLog() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new OutgoingEventTransportException("Transport error"))
                .when(mockDelegate).publish(any(Event.class));
        
        SilentEventPublisher silentPublisher = new SilentEventPublisher(mockDelegate);
        Event event = TestEvent.create("test");

        // when & then - should NOT throw
        assertDoesNotThrow(() -> silentPublisher.publish(event));
        
        verify(mockDelegate, times(1)).publish(event);
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException for null delegate")
    void shouldThrowIllegalArgumentExceptionForNullDelegate() {
        // when & then
        assertThrows(
                IllegalArgumentException.class,
                () -> new SilentEventPublisher(null)
        );
    }

    @Test
    @DisplayName("Should work with default constructor (logWarnings = true)")
    void shouldWorkWithDefaultConstructor() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new RuntimeException("Error"))
                .when(mockDelegate).publish(any(Event.class));
        
        SilentEventPublisher silentPublisher = new SilentEventPublisher(mockDelegate);
        Event event = TestEvent.create("test");

        // when & then - should NOT throw
        assertDoesNotThrow(() -> silentPublisher.publish(event));
    }

    @Test
    @DisplayName("Should work with logWarnings = false (DEBUG level)")
    void shouldWorkWithLogWarningsFalse() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new RuntimeException("Error"))
                .when(mockDelegate).publish(any(Event.class));
        
        SilentEventPublisher silentPublisher = new SilentEventPublisher(mockDelegate, false);
        Event event = TestEvent.create("test");

        // when & then - should NOT throw
        assertDoesNotThrow(() -> silentPublisher.publish(event));
    }

    @Test
    @DisplayName("Should work with RetryEventPublisher")
    void shouldWorkWithRetryEventPublisher() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new RuntimeException("Temporary error"))
                .when(mockDelegate).publish(any(Event.class));
        
        RetryEventPublisher retryPublisher = new RetryEventPublisher(
                mockDelegate, 2, java.time.Duration.ofMillis(10), 2.0
        );
        SilentEventPublisher silentPublisher = new SilentEventPublisher(retryPublisher);
        Event event = TestEvent.create("test");

        // when & then - should NOT throw (retry then silent)
        assertDoesNotThrow(() -> silentPublisher.publish(event));
        
        // Should have retried 3 times (1 initial + 2 retries)
        verify(mockDelegate, times(3)).publish(event);
    }

    @Test
    @DisplayName("Should preserve event type in error handling")
    void shouldPreserveEventTypeInErrorHandling() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new RuntimeException("Error"))
                .when(mockDelegate).publish(any(Event.class));
        
        SilentEventPublisher silentPublisher = new SilentEventPublisher(mockDelegate);
        Event event = TestEvent.create("specific-test");

        // when & then
        assertDoesNotThrow(() -> silentPublisher.publish(event));
        
        // Verify the event was passed correctly
        verify(mockDelegate).publish(argThat(e -> 
            e instanceof TestEvent && 
            ((TestEvent) e).getMessage().equals("specific-test")
        ));
    }

    @Test
    @DisplayName("Should handle multiple consecutive publish calls")
    void shouldHandleMultipleConsecutivePublishCalls() {
        // given
        EventPublisher mockDelegate = mock(EventPublisher.class);
        doThrow(new RuntimeException("Error 1"))
                .doNothing()
                .doThrow(new RuntimeException("Error 3"))
                .when(mockDelegate).publish(any(Event.class));
        
        SilentEventPublisher silentPublisher = new SilentEventPublisher(mockDelegate);

        // when & then - all should succeed silently
        assertDoesNotThrow(() -> silentPublisher.publish(TestEvent.create("test1")));
        assertDoesNotThrow(() -> silentPublisher.publish(TestEvent.create("test2")));
        assertDoesNotThrow(() -> silentPublisher.publish(TestEvent.create("test3")));
        
        verify(mockDelegate, times(3)).publish(any(Event.class));
    }
}
