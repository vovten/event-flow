package io.github.vovten.eventflow.event;

import io.github.vovten.eventflow.channel.BroadcastEventChannel;
import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.ExternalEventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link Envelope}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-05-05
 */
@DisplayName("Envelope Tests")
class EnvelopeTest {

    @Test
    @DisplayName("Should create envelope with default internal channel")
    void shouldCreateEnvelopeWithDefaultInternalChannel() {
        // Arrange
        PojoEvent pojoEvent = new PojoEvent("test");

        // Act
        Envelope<PojoEvent> envelope = Envelope.of(pojoEvent);

        // Assert
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(1, channels.size());
        assertEquals(InternalEventChannel.class, channels.get(0));
    }

    @Test
    @DisplayName("Should create envelope with external channel via factory method")
    void shouldCreateEnvelopeWithExternalChannelViaFactoryMethod() {
        // Arrange
        PojoEvent pojoEvent = new PojoEvent("test");

        // Act
        Envelope<PojoEvent> envelope = Envelope.of(pojoEvent, ExternalEventChannel.class);

        // Assert
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(1, channels.size());
        assertEquals(ExternalEventChannel.class, channels.get(0));
    }

    @Test
    @DisplayName("Should create envelope with multiple channels via factory method")
    void shouldCreateEnvelopeWithMultipleChannelsViaFactoryMethod() {
        // Arrange
        PojoEvent pojoEvent = new PojoEvent("test");

        // Act
        Envelope<PojoEvent> envelope = Envelope.of(pojoEvent, ExternalEventChannel.class, BroadcastEventChannel.class);

        // Assert
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(2, channels.size());
        assertEquals(ExternalEventChannel.class, channels.get(0));
        assertEquals(BroadcastEventChannel.class, channels.get(1));
    }

    @Test
    @DisplayName("Should create envelope with traceId and channels via factory method")
    void shouldCreateEnvelopeWithTraceIdAndChannelsViaFactoryMethod() {
        // Arrange
        PojoEvent pojoEvent = new PojoEvent("test");

        // Act
        Envelope<PojoEvent> envelope = Envelope.of(pojoEvent, "trace-123", ExternalEventChannel.class, BroadcastEventChannel.class);

        // Assert
        assertEquals("trace-123", envelope.traceId());
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(2, channels.size());
        assertEquals(ExternalEventChannel.class, channels.get(0));
        assertEquals(BroadcastEventChannel.class, channels.get(1));
    }

    @Test
    @DisplayName("Should create envelope with traceId only via factory method")
    void shouldCreateEnvelopeWithTraceIdOnlyViaFactoryMethod() {
        // Arrange
        PojoEvent pojoEvent = new PojoEvent("test");

        // Act
        Envelope<PojoEvent> envelope = Envelope.of(pojoEvent, "trace-xyz");

        // Assert
        assertEquals("trace-xyz", envelope.traceId());
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(1, channels.size());
        assertEquals(InternalEventChannel.class, channels.get(0));
    }

    @Test
    @DisplayName("Should resolve channels from @DomainEvent annotation on payload")
    void shouldResolveChannelsFromDomainEventAnnotationOnPayload() {
        // Arrange
        AnnotatedEvent annotatedEvent = new AnnotatedEvent("test");

        // Act
        Envelope<AnnotatedEvent> envelope = Envelope.of(annotatedEvent);

        // Assert
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(1, channels.size());
        assertEquals(ExternalEventChannel.class, channels.get(0));
    }

    @Test
    @DisplayName("Should resolve multiple channels from @DomainEvent annotation")
    void shouldResolveMultipleChannelsFromDomainEventAnnotation() {
        // Arrange
        MultiChannelEvent annotatedEvent = new MultiChannelEvent("test");

        // Act
        Envelope<MultiChannelEvent> envelope = Envelope.of(annotatedEvent);

        // Assert
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(3, channels.size());
        assertEquals(InternalEventChannel.class, channels.get(0));
        assertEquals(ExternalEventChannel.class, channels.get(1));
        assertEquals(BroadcastEventChannel.class, channels.get(2));
    }

    @Test
    @DisplayName("Factory method channels should take priority over @DomainEvent annotation")
    void factoryMethodChannelsShouldTakePriorityOverDomainEventAnnotation() {
        // Arrange
        AnnotatedEvent annotatedEvent = new AnnotatedEvent("test");

        // Act
        Envelope<AnnotatedEvent> envelope = Envelope.of(annotatedEvent, BroadcastEventChannel.class);

        // Assert
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(1, channels.size());
        assertEquals(BroadcastEventChannel.class, channels.get(0));
    }

    @Test
    @DisplayName("Should set correct payload type in metadata")
    void shouldSetCorrectPayloadTypeInMetadata() {
        // Arrange
        PojoEvent pojoEvent = new PojoEvent("test");

        // Act
        Envelope<PojoEvent> envelope = Envelope.of(pojoEvent);

        // Assert
        assertEquals(PojoEvent.class.getName(), envelope.getPayloadType());
    }

    @Test
    @DisplayName("Should throw exception when channels array is null")
    void shouldThrowExceptionWhenChannelsArrayIsNull() {
        // Arrange
        PojoEvent pojoEvent = new PojoEvent("test");

        // Assert
        assertThrows(NullPointerException.class, () ->
                Envelope.of(pojoEvent, (Class<? extends EventChannel>[]) null)
        );
    }

    @Test
    @DisplayName("Should throw exception when channels array is empty")
    void shouldThrowExceptionWhenChannelsArrayIsEmpty() {
        // Arrange
        PojoEvent pojoEvent = new PojoEvent("test");

        // Assert
        assertThrows(IllegalArgumentException.class, () ->
                Envelope.of(pojoEvent, (Class<? extends EventChannel>[]) new Class[0])
        );
    }

    @Test
    @DisplayName("Should throw exception when channels array is null with traceId")
    void shouldThrowExceptionWhenChannelsArrayIsNullWithTraceId() {
        // Arrange
        PojoEvent pojoEvent = new PojoEvent("test");

        // Assert
        assertThrows(NullPointerException.class, () ->
                Envelope.of(pojoEvent, "trace", (Class<? extends EventChannel>[]) null)
        );
    }

    /**
     * Simple POJO without any annotation — defaults to internal channel.
     */
    private static final class PojoEvent {
        private final String data;

        PojoEvent(String data) {
            this.data = data;
        }
    }

    /**
     * POJO with @DomainEvent annotation specifying external channel.
     */
    @DomainEvent(channels = ExternalEventChannel.class)
    private static final class AnnotatedEvent {
        private final String data;

        AnnotatedEvent(String data) {
            this.data = data;
        }
    }

    /**
     * POJO with @DomainEvent annotation specifying multiple channels.
     */
    @DomainEvent(channels = {InternalEventChannel.class, ExternalEventChannel.class, BroadcastEventChannel.class})
    private static final class MultiChannelEvent {
        private final String data;

        MultiChannelEvent(String data) {
            this.data = data;
        }
    }
}
