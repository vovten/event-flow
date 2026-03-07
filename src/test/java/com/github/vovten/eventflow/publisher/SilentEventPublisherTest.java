package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SilentEventPublisher.
 */
@DisplayName("SilentEventPublisher Tests")
class SilentEventPublisherTest {

    private EventPublisher delegate;
    private SilentEventPublisher silentPublisher;

    @BeforeEach
    void setUp() {
        delegate = mock(EventPublisher.class);
        silentPublisher = new SilentEventPublisher(delegate);
    }

    @Test
    @DisplayName("Should publish event successfully")
    void shouldPublishEventSuccessfully() {
        TestEvent event = new TestEvent();

        silentPublisher.publish(event);

        verify(delegate).publish(event);
    }

    @Test
    @DisplayName("Should swallow EventPublisherException")
    void shouldSwallowEventPublisherException() {
        TestEvent event = new TestEvent();
        doThrow(new EventPublisherException("Failed")).when(delegate).publish(event);

        assertDoesNotThrow(() -> silentPublisher.publish(event));
        verify(delegate).publish(event);
    }

    @Test
    @DisplayName("Should swallow RuntimeException")
    void shouldSwallowRuntimeException() {
        TestEvent event = new TestEvent();
        doThrow(new RuntimeException("Unexpected error")).when(delegate).publish(event);

        assertDoesNotThrow(() -> silentPublisher.publish(event));
    }

    @Test
    @DisplayName("Should not swallow Error")
    void shouldNotSwallowError() {
        TestEvent event = new TestEvent();
        doThrow(new OutOfMemoryError("OOM")).when(delegate).publish(event);

        assertThrows(OutOfMemoryError.class, () -> silentPublisher.publish(event));
    }

    static class TestEvent implements Event {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }
}
