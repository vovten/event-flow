package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import io.github.vovten.eventflow.transport.outgoing.LocalQueueOutTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ChannelEventPublisher.
 * @since 1.0.0
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
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>();
        OutTransport transport = new LocalQueueOutTransport(queue);
        EventChannel channel = new InternalEventChannel(transport);

        assertDoesNotThrow(() -> new ChannelEventPublisher(List.of(channel)));
    }

    @Test
    @DisplayName("Should send event to configured channel")
    void shouldSendEventToConfiguredChannel() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>();
        OutTransport transport = new LocalQueueOutTransport(queue);
        EventChannel channel = new InternalEventChannel(transport);

        ChannelEventPublisher publisher = new ChannelEventPublisher(List.of(channel));
        TestEvent event = new TestEvent();

        CompletableFuture<SendResults> future = publisher.publish(event);
        SendResults results = future.join();

        assertThat(results.isAllSuccess()).isTrue();
        Event sentEvent = queue.poll();
        assertThat(sentEvent).isEqualTo(event);
    }

    @Test
    @DisplayName("Should throw exception when channel not configured")
    void shouldThrowExceptionWhenChannelNotConfigured() {
        BlockingDeque<Event> queue = new LinkedBlockingDeque<>();
        OutTransport transport = new LocalQueueOutTransport(queue);
        EventChannel internalChannel = new InternalEventChannel(transport);

        ChannelEventPublisher publisher = new ChannelEventPublisher(List.of(internalChannel));

        TestEventWithExternalChannel event = new TestEventWithExternalChannel();

        assertThatThrownBy(() -> publisher.publish(event).join())
                .hasCauseInstanceOf(EventPublisherConfigException.class)
                .hasStackTraceContaining("ExternalEventChannel");
    }

    @Test
    @DisplayName("Should complete exceptionally when send fails")
    void shouldCompleteExceptionallyWhenSendFails() {
        OutTransport transport = new FailingOutTransport();
        EventChannel channel = new InternalEventChannel(transport);

        ChannelEventPublisher publisher = new ChannelEventPublisher(List.of(channel));
        TestEvent event = new TestEvent();

        CompletableFuture<SendResults> future = publisher.publish(event);
        SendResults results = future.join();

        assertThat(results.isAllFailure()).isTrue();
    }

    static class TestEvent extends AbstractTraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @Override
        public List<Class<? extends EventChannel>> channels() {
            return List.of(InternalEventChannel.class);
        }
    }

    static class TestEventWithExternalChannel extends AbstractTraceableEvent {
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
        public List<OutTransport> transports() {
            return List.of();
        }

        @Override
        public CompletableFuture<SendResults> send(Event event) {
            return CompletableFuture.completedFuture(SendResults.empty());
        }
    }

    static class FailingOutTransport implements OutTransport {
        @Override
        public String name() {
            return "failing";
        }

        @Override
        public CompletableFuture<SendResult> send(Event event) {
            return CompletableFuture.failedFuture(new RuntimeException("Send failed"));
        }
    }
}
