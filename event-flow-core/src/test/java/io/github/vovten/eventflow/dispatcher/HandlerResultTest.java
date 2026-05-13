package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.registry.EventHandlerInvocationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class HandlerResultTest {

    @Test
    @DisplayName("Should extract root cause message from wrapped exception")
    void shouldExtractRootCauseMessage() {
        Throwable rootCause = new RuntimeException("original error");
        Throwable wrapper = new EventHandlerInvocationException(new Object(), new TestEvent(), rootCause);

        HandlerResult result = HandlerResult.failure("testHandler", wrapper);

        assertThat(result.errorDetails()).isEqualTo("original error");
    }

    @Test
    @DisplayName("Should use top-level message when there is no cause")
    void shouldUseTopLevelMessageWhenNoCause() {
        Throwable error = new RuntimeException("direct error");

        HandlerResult result = HandlerResult.failure("testHandler", error);

        assertThat(result.errorDetails()).isEqualTo("direct error");
    }

    @Test
    @DisplayName("Should use top-level message when cause has no message")
    void shouldUseTopLevelMessageWhenCauseHasNoMessage() {
        Throwable rootCause = new RuntimeException();
        Throwable wrapper = new RuntimeException("wrapper message", rootCause);

        HandlerResult result = HandlerResult.failure("testHandler", wrapper);

        assertThat(result.errorDetails()).isEqualTo("wrapper message");
    }

    @Test
    @DisplayName("Should return null for null error")
    void shouldReturnNullForNullError() {
        HandlerResult result = HandlerResult.failure("testHandler", (Throwable) null);

        assertThat(result.errorDetails()).isNull();
    }

    @Test
    @DisplayName("Should create success result")
    void shouldCreateSuccessResult() {
        HandlerResult result = HandlerResult.success("myHandler");

        assertThat(result.success()).isTrue();
        assertThat(result.handlerName()).isEqualTo("myHandler");
        assertThat(result.error()).isNull();
        assertThat(result.errorDetails()).isNull();
    }

    @Test
    @DisplayName("Should create failure with custom error details")
    void shouldCreateFailureWithCustomDetails() {
        HandlerResult result = HandlerResult.failure("myHandler", "custom error");

        assertThat(result.success()).isFalse();
        assertThat(result.handlerName()).isEqualTo("myHandler");
        assertThat(result.error()).isNull();
        assertThat(result.errorDetails()).isEqualTo("custom error");
    }

    private record TestEvent() implements TraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
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
