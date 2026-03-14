package com.github.vovten.eventflow.channel;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for EventChannel interface default methods.
 */
@DisplayName("EventChannel Tests")
class EventChannelTest {

    private OutgoingEventTransport transport1;
    private OutgoingEventTransport transport2;
    private TestEventChannel channel;
    private TestEvent testEvent;

    @BeforeEach
    void setUp() {
        transport1 = mock(OutgoingEventTransport.class);
        transport2 = mock(OutgoingEventTransport.class);
        channel = new TestEventChannel(List.of(transport1, transport2));
        testEvent = new TestEvent();
    }

    @Test
    @DisplayName("Should send event to all transports")
    void shouldSendEventToAllTransports() {
        channel.send(testEvent);

        verify(transport1).send(testEvent);
        verify(transport2).send(testEvent);
    }

    @Test
    @DisplayName("Should propagate exception from transport")
    void shouldPropagateExceptionFromTransport() {
        doThrow(new RuntimeException("Transport failed")).when(transport1).send(testEvent);

        assertThrows(RuntimeException.class, () -> channel.send(testEvent));
        verify(transport2, never()).send(any());
    }

    @Test
    @DisplayName("Should return channel name")
    void shouldReturnChannelName() {
        assertEquals("test-channel", channel.name());
    }

    @Test
    @DisplayName("Should return configured transports")
    void shouldReturnConfiguredTransports() {
        List<OutgoingEventTransport> transports = channel.transports();

        assertEquals(2, transports.size());
        assertTrue(transports.contains(transport1));
        assertTrue(transports.contains(transport2));
    }

    static class TestEventChannel implements EventChannel {
        private final List<OutgoingEventTransport> transports;

        TestEventChannel(List<OutgoingEventTransport> transports) {
            this.transports = transports;
        }

        @Override
        public String name() {
            return "test-channel";
        }

        @Override
        public List<OutgoingEventTransport> transports() {
            return transports;
        }
    }

    static class TestEvent extends AbstractTraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }
}
