package com.github.vovten.eventflow.channel;

import com.github.vovten.eventflow.transport.PublisherTransport;
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
        PublisherTransport transport = mock(PublisherTransport.class);
        InternalEventChannel channel = new InternalEventChannel(transport);

        assertEquals("internal", channel.name());
        assertEquals(List.of(transport), channel.transports());
    }

    @Test
    @DisplayName("Should create channel with multiple transports")
    void shouldCreateChannelWithMultipleTransports() {
        PublisherTransport transport1 = mock(PublisherTransport.class);
        PublisherTransport transport2 = mock(PublisherTransport.class);
        List<PublisherTransport> transports = List.of(transport1, transport2);

        InternalEventChannel channel = new InternalEventChannel(transports);

        assertEquals("internal", channel.name());
        assertEquals(2, channel.transports().size());
        assertTrue(channel.transports().contains(transport1));
        assertTrue(channel.transports().contains(transport2));
    }

    @Test
    @DisplayName("Should return immutable transports list")
    void shouldReturnImmutableTransportsList() {
        PublisherTransport transport = mock(PublisherTransport.class);
        InternalEventChannel channel = new InternalEventChannel(transport);

        assertThrows(UnsupportedOperationException.class, () -> channel.transports().add(mock(PublisherTransport.class)));
    }
}
