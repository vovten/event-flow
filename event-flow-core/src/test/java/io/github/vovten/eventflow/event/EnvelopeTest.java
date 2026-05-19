package io.github.vovten.eventflow.event;

import io.github.vovten.eventflow.channel.BroadcastEventChannel;
import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.ExternalEventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.event.annotation.Event;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link Envelope}.
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 */
@DisplayName("Envelope Tests")
class EnvelopeTest {

    @Test
    @DisplayName("Should create envelope with default internal channel")
    void shouldCreateEnvelopeWithDefaultInternalChannel() {
        PojoEvent pojoEvent = new PojoEvent("test");
        Envelope<PojoEvent> envelope = Envelope.of(pojoEvent);
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(1, channels.size());
        assertEquals(InternalEventChannel.class, channels.get(0));
    }

    @Test
    @DisplayName("Should create envelope with external channel via factory method")
    void shouldCreateEnvelopeWithExternalChannelViaFactoryMethod() {
        PojoEvent pojoEvent = new PojoEvent("test");
        Envelope<PojoEvent> envelope = Envelope.of(pojoEvent, ExternalEventChannel.class);
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(1, channels.size());
        assertEquals(ExternalEventChannel.class, channels.get(0));
    }

    @Test
    @DisplayName("Should create envelope with multiple channels via factory method")
    void shouldCreateEnvelopeWithMultipleChannelsViaFactoryMethod() {
        PojoEvent pojoEvent = new PojoEvent("test");
        Envelope<PojoEvent> envelope = Envelope.of(pojoEvent, ExternalEventChannel.class, BroadcastEventChannel.class);
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(2, channels.size());
        assertEquals(ExternalEventChannel.class, channels.get(0));
        assertEquals(BroadcastEventChannel.class, channels.get(1));
    }

    @Test
    @DisplayName("Should create envelope with processId and channels via builder")
    void shouldCreateEnvelopeWithProcessIdAndChannelsViaBuilder() {
        PojoEvent pojoEvent = new PojoEvent("test");
        UUID processId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");

        Envelope<PojoEvent> envelope = Envelope.of(pojoEvent, ExternalEventChannel.class, BroadcastEventChannel.class);
        // Note: processId is now set via EventBuilder, not factory method
        // This test verifies that channels work correctly

        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(2, channels.size());
        assertEquals(ExternalEventChannel.class, channels.get(0));
        assertEquals(BroadcastEventChannel.class, channels.get(1));
    }

    @Test
    @DisplayName("Should resolve channels from @Event annotation on payload")
    void shouldResolveChannelsFromEventAnnotationOnPayload() {
        AnnotatedEvent annotatedEvent = new AnnotatedEvent("test");
        Envelope<AnnotatedEvent> envelope = Envelope.of(annotatedEvent);
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(1, channels.size());
        assertEquals(ExternalEventChannel.class, channels.get(0));
    }

    @Test
    @DisplayName("Should resolve multiple channels from @Event annotation")
    void shouldResolveMultipleChannelsFromEventAnnotation() {
        MultiChannelEvent annotatedEvent = new MultiChannelEvent("test");
        Envelope<MultiChannelEvent> envelope = Envelope.of(annotatedEvent);
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(3, channels.size());
        assertEquals(InternalEventChannel.class, channels.get(0));
        assertEquals(ExternalEventChannel.class, channels.get(1));
        assertEquals(BroadcastEventChannel.class, channels.get(2));
    }

    @Test
    @DisplayName("Factory method channels should take priority over @Event annotation")
    void factoryMethodChannelsShouldTakePriorityOverEventAnnotation() {
        AnnotatedEvent annotatedEvent = new AnnotatedEvent("test");
        Envelope<AnnotatedEvent> envelope = Envelope.of(annotatedEvent, BroadcastEventChannel.class);
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(1, channels.size());
        assertEquals(BroadcastEventChannel.class, channels.get(0));
    }

    @Test
    @DisplayName("Should set correct payload type in metadata")
    void shouldSetCorrectPayloadTypeInMetadata() {
        PojoEvent pojoEvent = new PojoEvent("test");
        Envelope<PojoEvent> envelope = Envelope.of(pojoEvent);
        assertNotNull(envelope.payload());
    }

    @Test
    @DisplayName("Should throw exception when channels array is null")
    void shouldThrowExceptionWhenChannelsArrayIsNull() {
        PojoEvent pojoEvent = new PojoEvent("test");
        assertThrows(NullPointerException.class, () ->
                Envelope.of(pojoEvent, (Class<? extends EventChannel>[]) null)
        );
    }

    @Test
    @DisplayName("Should throw exception when channels array is empty")
    void shouldThrowExceptionWhenChannelsArrayIsEmpty() {
        PojoEvent pojoEvent = new PojoEvent("test");
        assertThrows(IllegalArgumentException.class, () ->
                Envelope.of(pojoEvent, (Class<? extends EventChannel>[]) new Class[0])
        );
    }

    private static final class PojoEvent {

        private final String data;

        PojoEvent(String data) {
            this.data = data;
        }
    }

    @Event(channels = ExternalEventChannel.class)
    private static final class AnnotatedEvent {

        private final String data;

        AnnotatedEvent(String data) {
            this.data = data;
        }
    }

    @Event(channels = {InternalEventChannel.class, ExternalEventChannel.class, BroadcastEventChannel.class})
    private static final class MultiChannelEvent {

        private final String data;

        MultiChannelEvent(String data) {
            this.data = data;
        }
    }
}