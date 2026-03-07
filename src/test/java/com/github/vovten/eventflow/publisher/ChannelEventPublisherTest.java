package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;
import com.github.vovten.eventflow.test.TestEvent;
import com.github.vovten.eventflow.transport.OutgoingEventTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ChannelEventPublisher.
 */
@DisplayName("ChannelEventPublisher Tests")
class ChannelEventPublisherTest {

    @Test
    @DisplayName("Should throw exception for empty channels list")
    void shouldThrowExceptionForEmptyChannelsList() {
        assertThrows(IllegalArgumentException.class, () -> new ChannelEventPublisher(List.of()));
    }

    @Test
    @DisplayName("Should throw exception for null channels list")
    void shouldThrowExceptionForNullChannelsList() {
        assertThrows(IllegalArgumentException.class, () -> new ChannelEventPublisher(null));
    }

    @Test
    @DisplayName("Should create publisher with valid channels")
    void shouldCreatePublisherWithValidChannels() {
        EventChannel channel = mock(EventChannel.class);
        when(channel.name()).thenReturn("test");
        when(channel.transports()).thenReturn(List.of());

        assertDoesNotThrow(() -> new ChannelEventPublisher(List.of(channel)));
    }

    @Test
    @DisplayName("Should send event to configured channel")
    void shouldSendEventToConfiguredChannel() {
        OutgoingEventTransport transport = mock(OutgoingEventTransport.class);
        EventChannel channel = new InternalEventChannel(transport);

        ChannelEventPublisher publisher = new ChannelEventPublisher(List.of(channel));
        TestEvent event = new TestEvent();

        publisher.publish(event);

        verify(transport).send(event);
    }

    @Test
    @DisplayName("Should throw exception when channel not configured")
    void shouldThrowExceptionWhenChannelNotConfigured() {
        OutgoingEventTransport transport = mock(OutgoingEventTransport.class);
        EventChannel internalChannel = new InternalEventChannel(transport);

        ChannelEventPublisher publisher = new ChannelEventPublisher(List.of(internalChannel));

        TestEventWithExternalChannel event = new TestEventWithExternalChannel();

        EventPublisherConfigException exception = assertThrows(
                EventPublisherConfigException.class,
                () -> publisher.publish(event)
        );
        assertTrue(exception.getMessage().contains("ExternalEventChannel"));
    }

    @Test
    @DisplayName("Should wrap send exception in EventPublisherException")
    void shouldWrapSendExceptionInEventPublisherException() {
        OutgoingEventTransport transport = mock(OutgoingEventTransport.class);
        doThrow(new RuntimeException("Send failed")).when(transport).send(any());
        EventChannel channel = new InternalEventChannel(transport);

        ChannelEventPublisher publisher = new ChannelEventPublisher(List.of(channel));
        TestEvent event = new TestEvent();

        EventPublisherException exception = assertThrows(
                EventPublisherException.class,
                () -> publisher.publish(event)
        );
        assertTrue(exception.getMessage().contains("TestEvent"));
        assertTrue(exception.getMessage().contains("internal"));
    }

    static class TestEvent implements Event {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @Override
        public List<Class<? extends EventChannel>> channels() {
            return List.of(InternalEventChannel.class);
        }
    }

    static class TestEventWithExternalChannel implements Event {
        @Override
        public Class<? extends Event> type() {
            return TestEventWithExternalChannel.class;
        }

        @Override
        public List<Class<? extends EventChannel>> channels() {
            return List.of(ExternalEventChannel.class);
        }
    }

    static class ExternalEventChannel implements EventChannel {
        @Override
        public String name() {
            return "external";
        }

        @Override
        public List<OutgoingEventTransport> transports() {
            return List.of();
        }
    }
}
