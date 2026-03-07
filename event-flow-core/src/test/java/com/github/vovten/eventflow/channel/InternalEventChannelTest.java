package com.github.vovten.eventflow.channel;

import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for InternalEventChannel.
 */
@DisplayName("InternalEventChannel Tests")
class InternalEventChannelTest {

    @Test
    @DisplayName("Should create channel with single transport")
    void shouldCreateChannelWithSingleTransport() {
        OutgoingEventTransport transport = mock(OutgoingEventTransport.class);
        InternalEventChannel channel = new InternalEventChannel(transport);

        assertEquals("internal", channel.name());
        assertEquals(List.of(transport), channel.transports());
    }

    @Test
    @DisplayName("Should create channel with multiple transports")
    void shouldCreateChannelWithMultipleTransports() {
        OutgoingEventTransport transport1 = mock(OutgoingEventTransport.class);
        OutgoingEventTransport transport2 = mock(OutgoingEventTransport.class);
        List<OutgoingEventTransport> transports = List.of(transport1, transport2);

        InternalEventChannel channel = new InternalEventChannel(transports);

        assertEquals("internal", channel.name());
        assertEquals(2, channel.transports().size());
        assertTrue(channel.transports().contains(transport1));
        assertTrue(channel.transports().contains(transport2));
    }

    @Test
    @DisplayName("Should return immutable transports list")
    void shouldReturnImmutableTransportsList() {
        OutgoingEventTransport transport = mock(OutgoingEventTransport.class);
        InternalEventChannel channel = new InternalEventChannel(transport);

        assertThrows(UnsupportedOperationException.class, () -> channel.transports().add(mock(OutgoingEventTransport.class)));
    }
}
