package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.transport.outgoing.LocalQueueOutTransport;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link EventPublisherBuilder}.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
class EventPublisherBuilderTest {

    private EventChannel channel;

    @BeforeEach
    void setUp() {
        channel = new InternalEventChannel(new LocalQueueOutTransport(new LinkedBlockingDeque<>(1000)));
    }

    @Test
    @DisplayName("Should create builder")
    void shouldCreateBuilder() {
        assertNotNull(EventPublisherBuilder.create());
    }

    @Test
    @DisplayName("Should build publisher with channel")
    void shouldBuildPublisherWithChannel() {
        EventPublisher publisher = EventPublisherBuilder.create(channel).build();
        assertNotNull(publisher);
        assertInstanceOf(ChannelEventPublisher.class, publisher);
    }

    @Test
    @DisplayName("Should build publisher with multiple channels")
    void shouldBuildPublisherWithMultipleChannels() {
        EventPublisher publisher = EventPublisherBuilder.create(channel, channel).build();
        assertNotNull(publisher);
        assertInstanceOf(ChannelEventPublisher.class, publisher);
    }

    @Test
    @DisplayName("Should build publisher with retry")
    void shouldBuildPublisherWithRetry() {
        EventPublisher publisher = EventPublisherBuilder.create(channel)
                .retryable()
                .build();

        assertNotNull(publisher);
        assertInstanceOf(RetryEventPublisher.class, publisher);
    }

    @Test
    @DisplayName("Should build publisher with custom retry")
    void shouldBuildPublisherWithCustomRetry() {
        EventPublisher publisher = EventPublisherBuilder.create(channel)
                .retryable(5, Duration.ofMillis(200), 1.5)
                .build();

        assertNotNull(publisher);
        assertInstanceOf(RetryEventPublisher.class, publisher);
    }

    @Test
    @DisplayName("Should build publisher with custom decorator")
    void shouldBuildPublisherWithCustomDecorator() {
        EventPublisherBuilder.DecoratorFunction decorator = pub -> event -> CompletableFuture.completedFuture(SendResults.empty());

        EventPublisher publisher = EventPublisherBuilder.create(channel)
                .withDecorator(decorator)
                .build();

        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should build publisher with multiple decorators")
    void shouldBuildPublisherWithMultipleDecorators() {
        EventPublisherBuilder.DecoratorFunction decorator1 = pub -> event -> CompletableFuture.completedFuture(SendResults.empty());
        EventPublisherBuilder.DecoratorFunction decorator2 = pub -> event -> CompletableFuture.completedFuture(SendResults.empty());

        EventPublisher publisher = EventPublisherBuilder.create(channel)
                .withDecorator(decorator1)
                .withDecorator(decorator2)
                .retryable()
                .build();

        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should throw exception when building without channels")
    void shouldThrowExceptionWhenBuildingWithoutChannels() {
        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
                EventPublisherBuilder.create().build()
        );
        assertEquals("At least one channel must be configured", exception.getMessage());
    }

    @Test
    @DisplayName("Should build and log publisher")
    void shouldBuildAndLogPublisher() {
        EventPublisher publisher = EventPublisherBuilder.create(channel)
                .retryable()
                .buildAndLog();

        assertNotNull(publisher);
    }

    @Test
    @DisplayName("Should build publisher with all features")
    void shouldBuildPublisherWithAllFeatures() {
        EventPublisherBuilder.DecoratorFunction decorator = pub -> event -> CompletableFuture.completedFuture(SendResults.empty());

        EventPublisher publisher = EventPublisherBuilder.create(channel)
                .retryable()
                .withDecorator(decorator)
                .build();

        assertNotNull(publisher);
    }
}
