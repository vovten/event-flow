package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.ExternalEventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.retry.RetryKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("SuccessAck Tests")
class SuccessAckTest {

    private static final UUID EVENT_ID = UUID.randomUUID();
    private static final UUID ORIGINAL_EVENT_ID = UUID.randomUUID();
    private static final UUID PROCESS_ID = UUID.randomUUID();
    private static final String EVENT_TYPE = "com.example.OrderCreated";
    private static final String SERVICE = "order-service";
    private static final Instant OCCURRED_AT = Instant.now();
    private static final List<Class<? extends EventChannel>> CHANNELS =
            List.of(InternalEventChannel.class, ExternalEventChannel.class);

    @Test
    @DisplayName("Should expose retry metadata from the canonical constructor")
    void shouldExposeRetryMetadata() {
        SuccessAck ack = new SuccessAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                CHANNELS, PROCESS_ID, OCCURRED_AT,
                RetryKind.PROCESSING, 4);

        assertThat(ack.eventId()).isEqualTo(EVENT_ID);
        assertThat(ack.originalEventId()).isEqualTo(ORIGINAL_EVENT_ID);
        assertThat(ack.eventType()).isEqualTo(EVENT_TYPE);
        assertThat(ack.originalService()).isEqualTo(SERVICE);
        assertThat(ack.sourceChannels()).isEqualTo(CHANNELS);
        assertThat(ack.processId()).isEqualTo(PROCESS_ID);
        assertThat(ack.occurredAt()).isEqualTo(OCCURRED_AT);
        assertThat(ack.retryKind()).isEqualTo(RetryKind.PROCESSING);
        assertThat(ack.attempt()).isEqualTo(4);
    }

    @Test
    @DisplayName("Should allow a delivery retry context")
    void shouldAllowDeliveryRetryContext() {
        SuccessAck ack = new SuccessAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                CHANNELS, PROCESS_ID, OCCURRED_AT,
                RetryKind.DELIVERY, 2);

        assertThat(ack.retryKind()).isEqualTo(RetryKind.DELIVERY);
        assertThat(ack.attempt()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should apply legacy constructor defaults")
    void shouldApplyLegacyConstructorDefaults() {
        SuccessAck ack = new SuccessAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                CHANNELS, PROCESS_ID, OCCURRED_AT);

        assertThat(ack.retryKind()).isEqualTo(RetryKind.PROCESSING);
        assertThat(ack.attempt()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should implement lifecycle ack and traceable event contracts")
    void shouldImplementLifecycleAckAndTraceableEvent() {
        SuccessAck ack = new SuccessAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                CHANNELS, PROCESS_ID, OCCURRED_AT);

        assertThat(ack).isInstanceOf(LifecycleAckEvent.class);
        assertThat(ack.type()).isEqualTo(SuccessAck.class);
        assertThat(ack.channels()).isEqualTo(CHANNELS);
        assertThat(ack.eventId()).isEqualTo(EVENT_ID);
    }

    @Test
    @DisplayName("Should allow a null original service")
    void shouldAllowNullOriginalService() {
        SuccessAck ack = new SuccessAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, null,
                CHANNELS, PROCESS_ID, OCCURRED_AT);

        assertThat(ack.originalService()).isNull();
    }

    @Test
    @DisplayName("Should reject invalid attempt")
    void shouldRejectInvalidAttempt() {
        assertThatThrownBy(() -> new SuccessAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                CHANNELS, PROCESS_ID, OCCURRED_AT,
                RetryKind.PROCESSING, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attempt");
    }

    @Test
    @DisplayName("Should reject null retry kind")
    void shouldRejectNullRetryKind() {
        assertThatThrownBy(() -> new SuccessAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                CHANNELS, PROCESS_ID, OCCURRED_AT,
                null, 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("retryKind");
    }
}
