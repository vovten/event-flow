package com.github.vovten.eventflow;

import com.github.vovten.eventflow.channel.InternalEventChannel;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Event interface and implementations
 */
class EventIntegrationTest {

    @Test
    @DisplayName("Should serialize and deserialize event correctly")
    void shouldSerializeAndDeserializeEventCorrectly() {
        // given
        TestEvent originalEvent = TestEvent.create("Serialization test");

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
        TestEvent event = TestEvent.create();

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
        TestEvent event = TestEvent.create();

        // when
        Class<? extends Event> type = event.type();

        // then
        assertEquals(TestEvent.class, type);
    }

    @Test
    @DisplayName("Should produce consistent JSON output")
    void shouldProduceConsistentJsonOutput() {
        LocalDateTime dateTime = LocalDateTime.of(2026, 3, 4, 12, 0, 0);
        // given
        TestEvent event1 = TestEvent.create("same-id", "same-message", dateTime);
        TestEvent event2 = TestEvent.create("same-id", "same-message", dateTime);

        // when
        String json1 = event1.asJson();
        String json2 = event2.asJson();

        // then
        assertEquals(json1, json2);
    }
}
