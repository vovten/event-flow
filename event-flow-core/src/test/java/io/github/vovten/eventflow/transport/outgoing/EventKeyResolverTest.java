package io.github.vovten.eventflow.transport.outgoing;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * Tests for {@link EventKeyResolver}.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("EventKeyResolver Tests")
class EventKeyResolverTest {

    @Test
    @DisplayName("none() should always return null")
    void noneShouldAlwaysReturnNull() {
        EventKeyResolver resolver = EventKeyResolver.none();

        assertThat(resolver.resolveKey(Envelope.of("payload"))).isNull();
        assertThat(resolver.resolveKey(new TestEvent("1", "msg"))).isNull();
    }

    @Test
    @DisplayName("processId() should return processId for envelope with processId")
    void processIdShouldReturnProcessIdForEnvelope() {
        UUID processId = UUID.randomUUID();
        EventKeyResolver resolver = EventKeyResolver.processId();

        assertThat(resolver.resolveKey(Envelope.of("payload", processId)))
                .isEqualTo(processId.toString());
    }

    @Test
    @DisplayName("processId() should return null when processId is absent")
    void processIdShouldReturnNullWhenAbsent() {
        EventKeyResolver resolver = EventKeyResolver.processId();

        assertThat(resolver.resolveKey(Envelope.of("payload"))).isNull();
        assertThat(resolver.resolveKey(new TestEvent("1", "msg"))).isNull();
    }

    @Test
    @DisplayName("eventType() should return event type class name")
    void eventTypeShouldReturnClassName() {
        EventKeyResolver resolver = EventKeyResolver.eventType();
        Event envelope = Envelope.of("payload");

        assertThat(resolver.resolveKey(envelope)).isEqualTo(envelope.type().getName());
        assertThat(resolver.resolveKey(new TestEvent("1", "msg"))).isEqualTo(TestEvent.class.getName());
    }

    @Test
    @DisplayName("metadata() should return named metadata value")
    void metadataShouldReturnNamedValue() {
        EventKeyResolver resolver = EventKeyResolver.metadata("orderId");
        Envelope<String> envelope = Envelope.of("payload")
                .withAdditionalMetadata("orderId", "order-123");

        assertThat(resolver.resolveKey(envelope)).isEqualTo("order-123");
    }

    @Test
    @DisplayName("metadata() should return null when entry is absent or event is not an envelope")
    void metadataShouldReturnNullWhenAbsent() {
        EventKeyResolver resolver = EventKeyResolver.metadata("orderId");

        assertThat(resolver.resolveKey(Envelope.of("payload"))).isNull();
        assertThat(resolver.resolveKey(new TestEvent("1", "msg"))).isNull();
    }

    @Test
    @DisplayName("metadata() should reject null metadata key")
    void metadataShouldRejectNullKey() {
        assertThatNullPointerException().isThrownBy(() -> EventKeyResolver.metadata(null));
    }

    @Test
    @DisplayName("Should support custom resolver implementation (lambda extracting payload field)")
    void shouldSupportCustomResolverImplementation() {
        // A custom resolver is just a function from Event to String:
        EventKeyResolver custom = event -> {
            if (event instanceof Envelope<?> envelope && envelope.payload() instanceof OrderCreated order) {
                return order.orderId();
            }
            return null;
        };

        assertThat(custom.resolveKey(Envelope.of(new OrderCreated("order-123"))))
                .isEqualTo("order-123");
        // falls back to null (round-robin) for events without an order id
        assertThat(custom.resolveKey(Envelope.of("plain-payload"))).isNull();
    }

    private record OrderCreated(String orderId) {
    }
}