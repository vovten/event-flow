package com.github.vovten.eventflow.channel;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.OutTransport;
import com.github.vovten.eventflow.transport.SendResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for EventChannel interface default methods.
 */
@DisplayName("EventChannel Tests")
class EventChannelTest {

    private OutTransport transport1;
    private OutTransport transport2;
    private TestEventChannel channel;
    private TestEvent testEvent;

    @BeforeEach
    void setUp() {
        transport1 = mock(OutTransport.class);
        transport2 = mock(OutTransport.class);
        channel = new TestEventChannel(List.of(transport1, transport2));
        testEvent = new TestEvent();
    }

    @Test
    @DisplayName("Should send event to all transports")
    void shouldSendEventToAllTransports() {
        when(transport1.send(testEvent)).thenReturn(CompletableFuture.completedFuture(SendResult.success("dest1")));
        when(transport2.send(testEvent)).thenReturn(CompletableFuture.completedFuture(SendResult.success("dest2")));

        CompletableFuture<List<SendResult>> future = channel.send(testEvent);
        List<SendResult> results = future.join();

        assertThat(results).hasSize(2);
        verify(transport1).send(testEvent);
        verify(transport2).send(testEvent);
    }

    @Test
    @DisplayName("Should complete exceptionally when transport fails")
    void shouldCompleteExceptionallyWhenTransportFails() {
        when(transport1.send(testEvent)).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Transport failed")));
        when(transport2.send(testEvent)).thenReturn(CompletableFuture.completedFuture(SendResult.success("dest2")));

        CompletableFuture<List<SendResult>> future = channel.send(testEvent);

        assertThat(future).isCompletedExceptionally();
    }

    @Test
    @DisplayName("Should return channel name")
    void shouldReturnChannelName() {
        assertEquals("test-channel", channel.name());
    }

    @Test
    @DisplayName("Should return configured transports")
    void shouldReturnConfiguredTransports() {
        List<OutTransport> transports = channel.transports();

        assertEquals(2, transports.size());
        assertTrue(transports.contains(transport1));
        assertTrue(transports.contains(transport2));
    }

    static class TestEventChannel extends AbstractEventChannel {
        TestEventChannel(List<OutTransport> transports) {
            super(transports);
        }

        @Override
        public String name() {
            return "test-channel";
        }
    }

    static class TestEvent extends AbstractTraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }
}
