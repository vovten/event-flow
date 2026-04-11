package io.github.vovten.eventflow.channel;

import io.github.vovten.eventflow.transport.OutTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for ExternalEventChannel.
 */
@DisplayName("ExternalEventChannel Tests")
class ExternalEventChannelTest {

    @Test
    @DisplayName("Should create channel with single transport")
    void shouldCreateChannelWithSingleTransport() {
        OutTransport transport = mock(OutTransport.class);
        ExternalEventChannel channel = new ExternalEventChannel(transport);

        assertEquals("external", channel.name());
        assertEquals(List.of(transport), channel.transports());
    }

    @Test
    @DisplayName("Should create channel with multiple transports")
    void shouldCreateChannelWithMultipleTransports() {
        OutTransport transport1 = mock(OutTransport.class);
        OutTransport transport2 = mock(OutTransport.class);
        List<OutTransport> transports = List.of(transport1, transport2);

        ExternalEventChannel channel = new ExternalEventChannel(transports);

        assertEquals("external", channel.name());
        assertEquals(2, channel.transports().size());
        assertTrue(channel.transports().contains(transport1));
        assertTrue(channel.transports().contains(transport2));
    }

    @Test
    @DisplayName("Should return immutable transports list")
    void shouldReturnImmutableTransportsList() {
        OutTransport transport = mock(OutTransport.class);
        ExternalEventChannel channel = new ExternalEventChannel(transport);

        assertThrows(UnsupportedOperationException.class, () -> channel.transports().add(mock(OutTransport.class)));
    }
}
