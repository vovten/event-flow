package io.github.vovten.eventflow.publisher;

import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LoggingEventPublisherTest {

    @Test
    @DisplayName("Should throw exception when origin is null")
    void shouldThrowExceptionWhenOriginIsNull() {
        assertThatThrownBy(() -> new LoggingEventPublisher(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("origin must not be null");
    }

    @Test
    @DisplayName("Should pass envelope event through")
    void shouldPassEnvelopeEvent() {
        LoggingEventPublisher underTest = new LoggingEventPublisher(e -> {
            assertThat(e).isInstanceOf(Envelope.class);
            return CompletableFuture.completedFuture(SendResults.empty());
        });

        Envelope<String> envelope = Envelope.of("test-payload");

        underTest.publish(envelope).join();
    }

    @Test
    @DisplayName("Should handle TraceableEvent")
    void shouldHandleTraceableEvent() {
        LoggingEventPublisher underTest = new LoggingEventPublisher(e -> {
            assertThat(e).isInstanceOf(TestTraceableEvent.class);
            return CompletableFuture.completedFuture(SendResults.empty());
        });

        TestTraceableEvent event = new TestTraceableEvent();
        underTest.publish(event).join();
    }

    @Test
    @DisplayName("Should truncate long payload")
    void shouldTruncateLongPayload() {
        LoggingEventPublisher underTest = new LoggingEventPublisher(e -> {
            return CompletableFuture.completedFuture(SendResults.empty());
        });

        StringBuilder longStr = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            longStr.append("verylongword");
        }
        Envelope<String> envelope = Envelope.of(longStr.toString());

        underTest.publish(envelope).join();
    }

    record TestTraceableEvent() implements TraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return TestTraceableEvent.class;
        }

        @Override
        public List<Class<? extends io.github.vovten.eventflow.channel.EventChannel>> channels() {
            return List.of(InternalEventChannel.class);
        }

        @Override
        public UUID eventId() {
            return UUID.randomUUID();
        }

        @Override
        public UUID processId() {
            return UUID.randomUUID();
        }

        @Override
        public Instant occurredAt() {
            return Instant.now();
        }
    }
}