package com.github.vovten.eventflow.channel;

import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for ExternalEventChannel.
 */
class ExternalEventChannelTest {

    @Test
    void testConstructorWithSingleTransport() {
        OutgoingEventTransport transport = mock(OutgoingEventTransport.class);
        ExternalEventChannel channel = new ExternalEventChannel(transport);

        assertEquals("external", channel.name());
        assertEquals(List.of(transport), channel.transports());
    }

    @Test
    void testConstructorWithMultipleTransports() {
        OutgoingEventTransport transport1 = mock(OutgoingEventTransport.class);
        OutgoingEventTransport transport2 = mock(OutgoingEventTransport.class);
        List<OutgoingEventTransport> transports = List.of(transport1, transport2);

        ExternalEventChannel channel = new ExternalEventChannel(transports);

        assertEquals("external", channel.name());
        assertEquals(2, channel.transports().size());
        assertTrue(channel.transports().contains(transport1));
        assertTrue(channel.transports().contains(transport2));
    }

    @Test
    void testTransportsReturnsImmutableList() {
        OutgoingEventTransport transport = mock(OutgoingEventTransport.class);
        ExternalEventChannel channel = new ExternalEventChannel(transport);

        assertThrows(UnsupportedOperationException.class, () -> channel.transports().add(mock(OutgoingEventTransport.class)));
    }
}
