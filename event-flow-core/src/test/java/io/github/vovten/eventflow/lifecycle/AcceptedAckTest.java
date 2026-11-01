package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.ExternalEventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("AcceptedAck Tests")
class AcceptedAckTest {

    private static final UUID EVENT_ID = UUID.randomUUID();
    private static final UUID ORIGINAL_EVENT_ID = UUID.randomUUID();
    private static final String EVENT_TYPE = "com.example.OrderCreated";
    private static final String SERVICE = "order-service";
    private static final Instant OCCURRED_AT = Instant.now();
    private static final List<Class<? extends EventChannel>> CHANNELS =
            List.of(InternalEventChannel.class, ExternalEventChannel.class);

    @Test
    @DisplayName("Should expose all delivery confirmation fields")
    void shouldExposeDeliveryConfirmationFields() {
        AcceptedAck ack = new AcceptedAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                2, 1_500L, CHANNELS, OCCURRED_AT);

        assertThat(ack.eventId()).isEqualTo(EVENT_ID);
        assertThat(ack.originalEventId()).isEqualTo(ORIGINAL_EVENT_ID);
        assertThat(ack.eventType()).isEqualTo(EVENT_TYPE);
        assertThat(ack.originalService()).isEqualTo(SERVICE);
        assertThat(ack.attempt()).isEqualTo(2);
        assertThat(ack.maxProcessingMs()).isEqualTo(1_500L);
        assertThat(ack.sourceChannels()).isEqualTo(CHANNELS);
        assertThat(ack.occurredAt()).isEqualTo(OCCURRED_AT);
    }

    @Test
    @DisplayName("Should implement lifecycle ack and traceable event contracts")
    void shouldImplementLifecycleAckAndTraceableEvent() {
        AcceptedAck ack = new AcceptedAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                1, 0L, CHANNELS, OCCURRED_AT);

        assertThat(ack).isInstanceOf(LifecycleAckEvent.class);
        assertThat(ack.type()).isEqualTo(AcceptedAck.class);
        assertThat(ack.channels()).isEqualTo(CHANNELS);
        assertThat(ack.eventId()).isEqualTo(EVENT_ID);
        assertThat(ack.processId()).isNull();
    }

    @Test
    @DisplayName("Should allow a null original service")
    void shouldAllowNullOriginalService() {
        AcceptedAck ack = new AcceptedAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, null,
                1, 0L, CHANNELS, OCCURRED_AT);

        assertThat(ack.originalService()).isNull();
    }

    @Test
    @DisplayName("Should reject invalid attempt")
    void shouldRejectInvalidAttempt() {
        assertThatThrownBy(() -> new AcceptedAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                0, 0L, CHANNELS, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attempt");
    }

    @Test
    @DisplayName("Should reject negative max processing time")
    void shouldRejectNegativeMaxProcessingTime() {
        assertThatThrownBy(() -> new AcceptedAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                1, -1L, CHANNELS, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxProcessingMs");
    }

    @Test
    @DisplayName("Should reject null mandatory fields")
    void shouldRejectNullMandatoryFields() {
        assertThatThrownBy(() -> new AcceptedAck(
                null, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                1, 0L, CHANNELS, OCCURRED_AT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("eventId");

        assertThatThrownBy(() -> new AcceptedAck(
                EVENT_ID, null, EVENT_TYPE, SERVICE,
                1, 0L, CHANNELS, OCCURRED_AT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("originalEventId");

        assertThatThrownBy(() -> new AcceptedAck(
                EVENT_ID, ORIGINAL_EVENT_ID, null, SERVICE,
                1, 0L, CHANNELS, OCCURRED_AT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("eventType");

        assertThatThrownBy(() -> new AcceptedAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                1, 0L, null, OCCURRED_AT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("sourceChannels");

        assertThatThrownBy(() -> new AcceptedAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                1, 0L, CHANNELS, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("occurredAt");
    }
}
