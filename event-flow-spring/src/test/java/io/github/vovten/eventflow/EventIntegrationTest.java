package io.github.vovten.eventflow;

import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Event interface and implementations
 * @since 1.0.0
 */
class EventIntegrationTest {

    @Test
    @DisplayName("Should serialize and deserialize event correctly")
    void shouldSerializeAndDeserializeEventCorrectly() {
        // given
        TestEvent originalEvent = new TestEvent("Serialization test");

        // when
        String json = originalEvent.asJson();

        // then
        assertNotNull(json);
        assertTrue(json.contains("Serialization test"));
        assertTrue(json.contains("TestEvent"));
    }

    @Test
    @DisplayName("Should return correct channel types")
    void shouldReturnCorrectChannelTypes() {
        // given
        TestEvent event = new TestEvent();

        // when
        var channels = event.channels();

        // then
        assertNotNull(channels);
        assertEquals(1, channels.size());
        assertEquals(InternalEventChannel.class, channels.get(0));
    }

    @Test
    @DisplayName("Should return event type class")
    void shouldReturnEventTypeClass() {
        // given
        TestEvent event = new TestEvent();

        // when
        Class<? extends Event> type = event.type();

        // then
        assertEquals(TestEvent.class, type);
    }

    @Test
    @DisplayName("Should produce consistent JSON output")
    void shouldProduceConsistentJsonOutput() {
        Instant dateTime = Instant.ofEpochSecond(1741089600); // 2026-03-04 12:00:00 UTC
        UUID sameUid = UUID.randomUUID();
        UUID sameProcessId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        // given
        TestEvent event1 = new TestEvent(sameUid, sameProcessId, "same-id", "same-message", dateTime);
        TestEvent event2 = new TestEvent(sameUid, sameProcessId, "same-id", "same-message", dateTime);

        // when
        String json1 = event1.asJson();
        String json2 = event2.asJson();

        // then
        assertEquals(json1, json2);
    }
}