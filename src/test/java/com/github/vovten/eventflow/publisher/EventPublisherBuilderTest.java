package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;
import com.github.vovten.eventflow.transport.outgoing.InMemoryOutgoingEventTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for EventPublisherBuilder.
 */
@DisplayName("EventPublisherBuilder Tests")
class EventPublisherBuilderTest {

    @Test
    @DisplayName("Should throw exception when building without channels")
    void shouldThrowExceptionWhenBuildingWithoutChannels() {
        assertThrows(IllegalStateException.class, () ->
                EventPublisherBuilder.channels()
                        .build());
    }

    @Test
    @DisplayName("Should build publisher with single channel")
    void shouldBuildPublisherWithSingleChannel() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .build();

        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should build publisher with multiple channels")
    void shouldBuildPublisherWithMultipleChannels() {
        EventChannel channel1 = mock(EventChannel.class);
        EventChannel channel2 = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(List.of(channel1, channel2))
                .build();

        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should build publisher with retry")
    void shouldBuildPublisherWithRetry() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable(3, Duration.ofMillis(100), 2.0)
                .build();

        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should build publisher with default retry")
    void shouldBuildPublisherWithDefaultRetry() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable()
                .build();

        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should build publisher with silent mode")
    void shouldBuildPublisherWithSilentMode() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .silent()
                .build();

        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should build publisher with transactional mode")
    void shouldBuildPublisherWithTransactionalMode() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .transactional()
                .build();

        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should build publisher with retry and silent")
    void shouldBuildPublisherWithRetryAndSilent() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable(3, Duration.ofMillis(100), 2.0)
                .silent()
                .build();

        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should build publisher with retry and transactional")
    void shouldBuildPublisherWithRetryAndTransactional() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable()
                .transactional()
                .build();

        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should build publisher with all options")
    void shouldBuildPublisherWithAllOptions() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable(5, Duration.ofMillis(50), 1.5)
                .silent()
                .transactional()
                .build();

        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should throw exception for null channel")
    void shouldThrowExceptionForNullChannel() {
        assertThrows(NullPointerException.class, () ->
                EventPublisherBuilder.channels((EventChannel) null)
                        .build());
    }

    @Test
    @DisplayName("Should build publisher with InternalEventChannel")
    void shouldBuildPublisherWithInternalEventChannel() {
        EventChannel channel = new InternalEventChannel(new InMemoryOutgoingEventTransport(100));

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .build();

        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should add channels with varargs")
    void shouldAddChannelsWithVarargs() {
        EventChannel channel1 = mock(EventChannel.class);
        EventChannel channel2 = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel1)
                .addChannels(channel2)
                .build();

        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should add channels with list")
    void shouldAddChannelsWithList() {
        EventChannel channel1 = mock(EventChannel.class);
        EventChannel channel2 = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel1)
                .addChannels(List.of(channel2))
                .build();

        assertNotNull(publisher);
    }
}
