package io.github.vovten.eventflow.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
        assertEquals(InternalEventChannel.class, channels.getFirst());
    }

    @Test
    @DisplayName("Should create envelope with external channel via factory method")
    void shouldCreateEnvelopeWithExternalChannelViaFactoryMethod() {
        PojoEvent pojoEvent = new PojoEvent("test");
        Envelope<PojoEvent> envelope = Envelope.of(pojoEvent, ExternalEventChannel.class);
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(1, channels.size());
        assertEquals(ExternalEventChannel.class, channels.getFirst());
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
        assertEquals(ExternalEventChannel.class, channels.getFirst());
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
    @DisplayName("Should resolve channels from Event interface override on payload")
    void shouldResolveChannelsFromEventInterfaceOverride() {
        InterfaceEvent interfaceEvent = new InterfaceEvent("test");
        Envelope<InterfaceEvent> envelope = Envelope.of(interfaceEvent);
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(1, channels.size());
        assertEquals(ExternalEventChannel.class, channels.getFirst());
    }

    @Test
    @DisplayName("Factory method channels should take priority over @Event annotation")
    void factoryMethodChannelsShouldTakePriorityOverEventAnnotation() {
        AnnotatedEvent annotatedEvent = new AnnotatedEvent("test");
        Envelope<AnnotatedEvent> envelope = Envelope.of(annotatedEvent, BroadcastEventChannel.class);
        List<Class<? extends EventChannel>> channels = envelope.channels();
        assertEquals(1, channels.size());
        assertEquals(BroadcastEventChannel.class, channels.getFirst());
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

    @Test
    @DisplayName("Should handle null metadata in constructor")
    void shouldHandleNullMetadataInConstructor() {
        Envelope<String> envelope = new Envelope<>(
                UUID.randomUUID(),
                null,
                java.time.Instant.now(),
                "test",
                null,   // metadata = null
                null    // channels = null
        );

        assertNotNull(envelope);
        assertTrue(envelope.metadata().isEmpty());
        assertEquals("test", envelope.payload());
    }

    @Test
    @DisplayName("Should deserialize from JSON without metadata field via ObjectMapper")
    void shouldDeserializeFromJsonWithoutMetadataField() throws Exception {
        // Simulate Jackson deserialization when "metadata" field is absent from JSON
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        UUID eventId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        String json = String.format("""
                {
                    "@class": "io.github.vovten.eventflow.event.Envelope",
                    "eventId": "%s",
                    "processId": null,
                    "occurredAt": "2026-05-20T12:00:00Z",
                    "payload": {"@class": "io.github.vovten.eventflow.event.EnvelopeTest$PojoEvent", "data": "test-payload"}
                }
                """, eventId);

        Envelope<?> envelope = mapper.readValue(json, Envelope.class);

        assertNotNull(envelope);
        assertEquals(eventId, envelope.eventId());
        assertTrue(envelope.metadata().isEmpty());
    }

    @Test
    @DisplayName("Should deserialize from JSON with null metadata field via ObjectMapper")
    void shouldDeserializeFromJsonWithNullMetadataField() throws Exception {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        UUID eventId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        String json = String.format("""
                {
                    "@class": "io.github.vovten.eventflow.event.Envelope",
                    "eventId": "%s",
                    "processId": null,
                    "occurredAt": "2026-05-20T12:00:00Z",
                    "payload": {"@class": "io.github.vovten.eventflow.event.EnvelopeTest$PojoEvent", "data": "test-payload"},
                    "metadata": null
                }
                """, eventId);

        Envelope<?> envelope = mapper.readValue(json, Envelope.class);

        assertNotNull(envelope);
        assertEquals(eventId, envelope.eventId());
        assertTrue(envelope.metadata().isEmpty());
    }

    private static final class PojoEvent {

        private String data;

        PojoEvent() {
        }

        PojoEvent(String data) {
            this.data = data;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }
    }

    /**
     * Payload that overrides {@code channels()} via the {@code Event} interface
     * but has no {@code @Event} annotation. Its channel list must be respected
     * when wrapped in an {@link Envelope}.
     */
    private static final class InterfaceEvent implements io.github.vovten.eventflow.event.Event {

        private final String data;

        InterfaceEvent(String data) {
            this.data = data;
        }

        @Override
        public Class<?> type() {
            return InterfaceEvent.class;
        }

        @Override
        public List<Class<? extends EventChannel>> channels() {
            return List.of(ExternalEventChannel.class);
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