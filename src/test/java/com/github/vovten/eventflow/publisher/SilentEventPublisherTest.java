package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SilentEventPublisher.
 */
class SilentEventPublisherTest {

    private EventPublisher delegate;
    private SilentEventPublisher silentPublisher;

    @BeforeEach
    void setUp() {
        delegate = mock(EventPublisher.class);
        silentPublisher = new SilentEventPublisher(delegate);
    }

    @Test
    void testPublish_Success() {
        TestEvent event = new TestEvent();

        silentPublisher.publish(event);

        verify(delegate).publish(event);
    }

    @Test
    void testPublish_DelegateThrowsException_Swallowed() {
        TestEvent event = new TestEvent();
        doThrow(new EventPublisherException("Failed")).when(delegate).publish(event);

        assertDoesNotThrow(() -> silentPublisher.publish(event));
        verify(delegate).publish(event);
    }

    @Test
    void testPublish_RuntimeException_Swallowed() {
        TestEvent event = new TestEvent();
        doThrow(new RuntimeException("Unexpected error")).when(delegate).publish(event);

        assertDoesNotThrow(() -> silentPublisher.publish(event));
    }

    @Test
    void testPublish_Error_NotSwallowed() {
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
