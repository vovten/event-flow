package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;
import com.github.vovten.eventflow.transport.outgoing.LocalQueueOutTransport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link EventPublisherBuilder}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("EventPublisherBuilder Tests")
class EventPublisherBuilderTest {

    private EventChannel channel;

    @BeforeEach
    void setUp() {
        channel = new InternalEventChannel(new LocalQueueOutTransport(new LinkedBlockingDeque<>(100)));
    }

    @Test
    @DisplayName("Should build simple publisher with channels")
    void shouldBuildSimplePublisherWithChannels() {
        // Act
        EventPublisher publisher = EventPublisherBuilder.channels(channel).build();

        // Assert
        assertNotNull(publisher);
        assertTrue(publisher instanceof ChannelEventPublisher);
    }

    @Test
    @DisplayName("Should build publisher with multiple channels")
    void shouldBuildPublisherWithMultipleChannels() {
        // Arrange
        EventChannel channel2 = new InternalEventChannel(new LocalQueueOutTransport(new LinkedBlockingDeque<>(100)));

        // Act
        EventPublisher publisher = EventPublisherBuilder.channels(channel, channel2).build();

        // Assert
        assertNotNull(publisher);
        assertTrue(publisher instanceof ChannelEventPublisher);
    }

    @Test
    @DisplayName("Should build publisher with channels list")
    void shouldBuildPublisherWithChannelsList() {
        // Arrange
        List<EventChannel> channels = List.of(channel, new InternalEventChannel(new LocalQueueOutTransport(new LinkedBlockingDeque<>(100))));

        // Act
        EventPublisher publisher = EventPublisherBuilder.channels(channels).build();

        // Assert
        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should add channels to builder")
    void shouldAddChannelsToBuilder() {
        // Arrange
        EventChannel channel2 = new InternalEventChannel(new LocalQueueOutTransport(new LinkedBlockingDeque<>(100)));

        // Act
        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .addChannels(channel2)
                .build();

        // Assert
        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should add channels list to builder")
    void shouldAddChannelsListToBuilder() {
        // Arrange
        List<EventChannel> channels = List.of(new InternalEventChannel(new LocalQueueOutTransport(new LinkedBlockingDeque<>(100))),
                new InternalEventChannel(new LocalQueueOutTransport(new LinkedBlockingDeque<>(100))));

        // Act
        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .addChannels(channels)
                .build();

        // Assert
        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should build publisher with default retry")
    void shouldBuildPublisherWithDefaultRetry() {
        // Act
        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable()
                .build();

        // Assert
        assertNotNull(publisher);
        assertTrue(publisher instanceof RetryEventPublisher);
    }

    @Test
    @DisplayName("Should build publisher with custom retry")
    void shouldBuildPublisherWithCustomRetry() {
        // Act
        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable(5, Duration.ofMillis(200), 1.5)
                .build();

        // Assert
        assertNotNull(publisher);
        assertTrue(publisher instanceof RetryEventPublisher);
    }

    @Test
    @DisplayName("Should build silent publisher")
    void shouldBuildSilentPublisher() {
        // Act
        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .silent()
                .build();

        // Assert
        assertNotNull(publisher);
        assertTrue(publisher instanceof SilentEventPublisher);
    }

    @Test
    @DisplayName("Should build publisher with retry and silent")
    void shouldBuildPublisherWithRetryAndSilent() {
        // Act
        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable()
                .silent()
                .build();

        // Assert
        assertNotNull(publisher);
        assertTrue(publisher instanceof SilentEventPublisher);
    }

    @Test
    @DisplayName("Should build publisher with custom decorator")
    void shouldBuildPublisherWithCustomDecorator() {
        // Arrange
        EventPublisherBuilder.DecoratorFunction decorator = pub -> event -> {};

        // Act
        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .withDecorator(decorator)
                .build();

        // Assert
        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should build publisher with multiple decorators")
    void shouldBuildPublisherWithMultipleDecorators() {
        // Arrange
        EventPublisherBuilder.DecoratorFunction decorator1 = pub -> event -> {};
        EventPublisherBuilder.DecoratorFunction decorator2 = pub -> event -> {};

        // Act
        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .withDecorator(decorator1)
                .withDecorator(decorator2)
                .retryable()
                .silent()
                .build();

        // Assert
        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should throw exception when building without channels")
    void shouldThrowExceptionWhenBuildingWithoutChannels() {
        // Assert
        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
                EventPublisherBuilder.channels().build()
        );
        assertEquals("At least one channel must be configured", exception.getMessage());
    }

    @Test
    @DisplayName("Should build and log publisher")
    void shouldBuildAndLogPublisher() {
        // Act
        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable()
                .silent()
                .buildAndLog();

        // Assert
        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should build publisher with all features")
    void shouldBuildPublisherWithAllFeatures() {
        // Arrange
        EventPublisherBuilder.DecoratorFunction decorator = pub -> new SilentEventPublisher(pub, false);

        // Act
        EventPublisher publisher = EventPublisherBuilder.channels(channel)
                .retryable(3, Duration.ofMillis(100), 2.0)
                .withDecorator(decorator)
                .silent()
                .build();

        // Assert
        assertNotNull(publisher);
    }
}
