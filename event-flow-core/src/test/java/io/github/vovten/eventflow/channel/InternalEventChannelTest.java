package io.github.vovten.eventflow.channel;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.outgoing.LocalQueueOutTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for InternalEventChannel.
 * @since 1.0.0
 */
@DisplayName("InternalEventChannel Tests")
class InternalEventChannelTest {

    @Test
    @DisplayName("Should create channel with single transport")
    void shouldCreateChannelWithSingleTransport() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>();
        OutTransport transport = new LocalQueueOutTransport(queue);
        InternalEventChannel channel = new InternalEventChannel(transport);

        assertEquals("internal", channel.name());
        assertEquals(List.of(transport), channel.transports());
    }

    @Test
    @DisplayName("Should create channel with multiple transports")
    void shouldCreateChannelWithMultipleTransports() {
        BlockingDeque<Event> queue1 = new LinkedBlockingDeque<>();
        BlockingDeque<Event> queue2 = new LinkedBlockingDeque<>();
        OutTransport transport1 = new LocalQueueOutTransport(queue1);
        OutTransport transport2 = new LocalQueueOutTransport(queue2);
        List<OutTransport> transports = List.of(transport1, transport2);

        InternalEventChannel channel = new InternalEventChannel(transports);

        assertEquals("internal", channel.name());
        assertEquals(2, channel.transports().size());
        assertTrue(channel.transports().contains(transport1));
        assertTrue(channel.transports().contains(transport2));
    }

    @Test
    @DisplayName("Should return immutable transports list")
    void shouldReturnImmutableTransportsList() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>();
        OutTransport transport = new LocalQueueOutTransport(queue);
        InternalEventChannel channel = new InternalEventChannel(transport);

        BlockingDeque<Event> otherQueue = new LinkedBlockingDeque<>();
        OutTransport otherTransport = new LocalQueueOutTransport(otherQueue);
        assertThrows(UnsupportedOperationException.class, () -> channel.transports().add(otherTransport));
    }
}
