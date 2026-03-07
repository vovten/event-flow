package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;
import com.github.vovten.eventflow.transport.outgoing.InMemoryOutgoingEventTransport;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for EventPublisherBuilder.
 */
class EventPublisherBuilderTest {

    @Test
    void testBuild_WithoutChannels_ThrowsException() {
        assertThrows(IllegalStateException.class, () ->
                EventPublisherBuilder.channels()
                        .build());
    }

    @Test
    void testBuild_WithChannels() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .build();

        assertNotNull(publisher);
    }

    @Test
    void testBuild_WithMultipleChannels() {
        EventChannel channel1 = mock(EventChannel.class);
        EventChannel channel2 = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(List.of(channel1, channel2))
                .build();

        assertNotNull(publisher);
    }

    @Test
    void testBuild_WithRetry() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable(3, Duration.ofMillis(100), 2.0)
                .build();

        assertNotNull(publisher);
    }

    @Test
    void testBuild_WithRetry_DefaultValues() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable()
                .build();

        assertNotNull(publisher);
    }

    @Test
    void testBuild_WithSilent() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .silent()
                .build();

        assertNotNull(publisher);
    }

    @Test
    void testBuild_WithTransactional() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .transactional()
                .build();

        assertNotNull(publisher);
    }

    @Test
    void testBuild_WithRetryAndSilent() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable(3, Duration.ofMillis(100), 2.0)
                .silent()
                .build();

        assertNotNull(publisher);
    }

    @Test
    void testBuild_WithRetryAndTransactional() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable()
                .transactional()
                .build();

        assertNotNull(publisher);
    }

    @Test
    void testBuild_WithAllOptions() {
        EventChannel channel = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable(5, Duration.ofMillis(50), 1.5)
                .silent()
                .transactional()
                .build();

        assertNotNull(publisher);
    }

    @Test
    void testBuild_NullChannel_ThrowsException() {
        assertThrows(NullPointerException.class, () ->
                EventPublisherBuilder.channels((EventChannel) null)
                        .build());
    }

    @Test
    void testBuild_WithInternalEventChannel() {
        EventChannel channel = new InternalEventChannel(new InMemoryOutgoingEventTransport(100));

        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .build();

        assertNotNull(publisher);
    }

    @Test
    void testAddChannels_Varargs() {
        EventChannel channel1 = mock(EventChannel.class);
        EventChannel channel2 = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel1)
                .addChannels(channel2)
                .build();

        assertNotNull(publisher);
    }

    @Test
    void testAddChannels_List() {
        EventChannel channel1 = mock(EventChannel.class);
        EventChannel channel2 = mock(EventChannel.class);

        EventPublisher publisher = EventPublisherBuilder.channels(channel1)
                .addChannels(List.of(channel2))
                .build();

        assertNotNull(publisher);
    }
}
