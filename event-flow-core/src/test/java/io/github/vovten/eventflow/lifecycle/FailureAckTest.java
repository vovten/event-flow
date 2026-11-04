package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.ExternalEventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.retry.RetryKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("FailureAck Tests")
class FailureAckTest {

    private static final UUID EVENT_ID = UUID.randomUUID();
    private static final UUID ORIGINAL_EVENT_ID = UUID.randomUUID();
    private static final UUID PROCESS_ID = UUID.randomUUID();
    private static final String EVENT_TYPE = "com.example.OrderCreated";
    private static final String SERVICE = "order-service";
    private static final String ERROR = "Handler failed";
    private static final Instant OCCURRED_AT = Instant.now();
    private static final List<Class<? extends EventChannel>> CHANNELS =
            List.of(InternalEventChannel.class, ExternalEventChannel.class);

    @Test
    @DisplayName("Should expose retry metadata from the canonical constructor")
    void shouldExposeRetryMetadata() {
        FailureAck ack = new FailureAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE, ERROR,
                CHANNELS, PROCESS_ID, OCCURRED_AT,
                RetryKind.PROCESSING, 3, false,
                List.of("PaymentHandler", "AuditHandler"),
                "java.sql.SQLException", 1_500L, "payment-policy");

        assertThat(ack.eventId()).isEqualTo(EVENT_ID);
        assertThat(ack.originalEventId()).isEqualTo(ORIGINAL_EVENT_ID);
        assertThat(ack.eventType()).isEqualTo(EVENT_TYPE);
        assertThat(ack.originalService()).isEqualTo(SERVICE);
        assertThat(ack.error()).isEqualTo(ERROR);
        assertThat(ack.sourceChannels()).isEqualTo(CHANNELS);
        assertThat(ack.processId()).isEqualTo(PROCESS_ID);
        assertThat(ack.occurredAt()).isEqualTo(OCCURRED_AT);
        assertThat(ack.retryKind()).isEqualTo(RetryKind.PROCESSING);
        assertThat(ack.attempt()).isEqualTo(3);
        assertThat(ack.retryable()).isFalse();
        assertThat(ack.failedHandlers()).containsExactly("PaymentHandler", "AuditHandler");
        assertThat(ack.errorType()).isEqualTo("java.sql.SQLException");
        assertThat(ack.retryAfterMs()).isEqualTo(1_500L);
        assertThat(ack.policyName()).isEqualTo("payment-policy");
    }

    @Test
    @DisplayName("Should apply legacy constructor defaults")
    void shouldApplyLegacyConstructorDefaults() {
        FailureAck ack = new FailureAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE, ERROR,
                CHANNELS, PROCESS_ID, OCCURRED_AT);

        assertThat(ack.retryKind()).isEqualTo(RetryKind.PROCESSING);
        assertThat(ack.attempt()).isEqualTo(1);
        assertThat(ack.retryable()).isTrue();
        assertThat(ack.failedHandlers()).isEmpty();
        assertThat(ack.errorType()).isNull();
        assertThat(ack.retryAfterMs()).isZero();
        assertThat(ack.policyName()).isNull();
    }

    @Test
    @DisplayName("Should implement lifecycle ack and traceable event contracts")
    void shouldImplementLifecycleAckAndTraceableEvent() {
        FailureAck ack = new FailureAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE, ERROR,
                CHANNELS, PROCESS_ID, OCCURRED_AT);

        assertThat(ack).isInstanceOf(LifecycleAckEvent.class);
        assertThat(ack.type()).isEqualTo(FailureAck.class);
        assertThat(ack.channels()).isEqualTo(CHANNELS);
        assertThat(ack.eventId()).isEqualTo(EVENT_ID);
    }

    @Test
    @DisplayName("Should defensively copy failed handler names")
    void shouldDefensivelyCopyFailedHandlers() {
        List<String> mutableHandlers = new ArrayList<>(List.of("FirstHandler"));

        FailureAck ack = new FailureAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE, ERROR,
                CHANNELS, PROCESS_ID, OCCURRED_AT,
                RetryKind.PROCESSING, 2, true, mutableHandlers, null, 0L, null);

        mutableHandlers.add("SecondHandler");

        assertThat(ack.failedHandlers()).containsExactly("FirstHandler");
    }

    @Test
    @DisplayName("Should reject invalid attempt")
    void shouldRejectInvalidAttempt() {
        assertThatThrownBy(() -> new FailureAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE, ERROR,
                CHANNELS, PROCESS_ID, OCCURRED_AT,
                RetryKind.PROCESSING, 0, true, List.of(), null, 0L, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attempt");
    }

    @Test
    @DisplayName("Should reject null retry kind")
    void shouldRejectNullRetryKind() {
        assertThatThrownBy(() -> new FailureAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE, ERROR,
                CHANNELS, PROCESS_ID, OCCURRED_AT,
                null, 1, true, List.of(), null, 0L, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("retryKind");
    }

    @Test
    @DisplayName("Should reject null failed handlers")
    void shouldRejectNullFailedHandlers() {
        assertThatThrownBy(() -> new FailureAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE, ERROR,
                CHANNELS, PROCESS_ID, OCCURRED_AT,
                RetryKind.PROCESSING, 1, true, null, null, 0L, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("failedHandlers");
    }

    @Test
    @DisplayName("Should reject a negative retry delay")
    void shouldRejectNegativeRetryDelay() {
        assertThatThrownBy(() -> new FailureAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE, ERROR,
                CHANNELS, PROCESS_ID, OCCURRED_AT,
                RetryKind.PROCESSING, 1, true, List.of(), null, -1L, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryAfterMs");
    }
}
