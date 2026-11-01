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

@DisplayName("NoHandlerAck Tests")
class NoHandlerAckTest {

    private static final UUID EVENT_ID = UUID.randomUUID();
    private static final UUID ORIGINAL_EVENT_ID = UUID.randomUUID();
    private static final String EVENT_TYPE = "com.example.OrderCreated";
    private static final String SERVICE = "order-service";
    private static final Instant OCCURRED_AT = Instant.now();
    private static final List<Class<? extends EventChannel>> CHANNELS =
            List.of(InternalEventChannel.class, ExternalEventChannel.class);

    @Test
    @DisplayName("Should expose all no-handler fields")
    void shouldExposeNoHandlerFields() {
        NoHandlerAck ack = new NoHandlerAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                3, CHANNELS, OCCURRED_AT);

        assertThat(ack.eventId()).isEqualTo(EVENT_ID);
        assertThat(ack.originalEventId()).isEqualTo(ORIGINAL_EVENT_ID);
        assertThat(ack.eventType()).isEqualTo(EVENT_TYPE);
        assertThat(ack.originalService()).isEqualTo(SERVICE);
        assertThat(ack.attempt()).isEqualTo(3);
        assertThat(ack.sourceChannels()).isEqualTo(CHANNELS);
        assertThat(ack.occurredAt()).isEqualTo(OCCURRED_AT);
    }

    @Test
    @DisplayName("Should implement lifecycle ack and traceable event contracts")
    void shouldImplementLifecycleAckAndTraceableEvent() {
        NoHandlerAck ack = new NoHandlerAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                1, CHANNELS, OCCURRED_AT);

        assertThat(ack).isInstanceOf(LifecycleAckEvent.class);
        assertThat(ack.type()).isEqualTo(NoHandlerAck.class);
        assertThat(ack.channels()).isEqualTo(CHANNELS);
        assertThat(ack.eventId()).isEqualTo(EVENT_ID);
        assertThat(ack.processId()).isNull();
    }

    @Test
    @DisplayName("Should allow a null original service")
    void shouldAllowNullOriginalService() {
        NoHandlerAck ack = new NoHandlerAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, null,
                1, CHANNELS, OCCURRED_AT);

        assertThat(ack.originalService()).isNull();
    }

    @Test
    @DisplayName("Should reject invalid attempt")
    void shouldRejectInvalidAttempt() {
        assertThatThrownBy(() -> new NoHandlerAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                0, CHANNELS, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attempt");
    }

    @Test
    @DisplayName("Should reject null mandatory fields")
    void shouldRejectNullMandatoryFields() {
        assertThatThrownBy(() -> new NoHandlerAck(
                null, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                1, CHANNELS, OCCURRED_AT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("eventId");

        assertThatThrownBy(() -> new NoHandlerAck(
                EVENT_ID, null, EVENT_TYPE, SERVICE,
                1, CHANNELS, OCCURRED_AT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("originalEventId");

        assertThatThrownBy(() -> new NoHandlerAck(
                EVENT_ID, ORIGINAL_EVENT_ID, null, SERVICE,
                1, CHANNELS, OCCURRED_AT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("eventType");

        assertThatThrownBy(() -> new NoHandlerAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                1, null, OCCURRED_AT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("sourceChannels");

        assertThatThrownBy(() -> new NoHandlerAck(
                EVENT_ID, ORIGINAL_EVENT_ID, EVENT_TYPE, SERVICE,
                1, CHANNELS, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("occurredAt");
    }
}
