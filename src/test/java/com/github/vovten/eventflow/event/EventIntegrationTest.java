package com.github.vovten.eventflow.event;

import com.github.vovten.eventflow.event.test.TestEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
    @DisplayName("Should return correct event bus types")
    void shouldReturnCorrectEventBusTypes() {
        // given
        TestEvent event = TestEvent.create();

        // when
        var eventBusTypes = event.eventBusTypes();

        // then
        assertNotNull(eventBusTypes);
        assertEquals(1, eventBusTypes.size());
        assertEquals(EventBus.INTERNAL, eventBusTypes.get(0));
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
        // given
        TestEvent event1 = TestEvent.create("same-id", "same-message");
        TestEvent event2 = TestEvent.create("same-id", "same-message");

        // when
        String json1 = event1.asJson();
        String json2 = event2.asJson();

        // then
        assertEquals(json1, json2);
    }
}
