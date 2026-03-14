package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link SilentEventPublisher}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("SilentEventPublisher Tests")
class SilentEventPublisherTest {

    @Test
    @DisplayName("Should throw exception when delegate is null")
    void shouldThrowExceptionWhenDelegateIsNull() {
        // Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                new SilentEventPublisher(null)
        );
        assertEquals("EventPublisher delegate must not be null", exception.getMessage());
    }

    @Test
    @DisplayName("Should throw exception when delegate is null with logWarnings")
    void shouldThrowExceptionWhenDelegateIsNullWithLogWarnings() {
        // Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                new SilentEventPublisher(null, true)
        );
        assertEquals("EventPublisher delegate must not be null", exception.getMessage());
    }

    @Test
    @DisplayName("Should publish event successfully")
    void shouldPublishEventSuccessfully() {
        // Arrange
        TestEvent event = new TestEvent("test");
        AtomicBoolean called = new AtomicBoolean(false);
        EventPublisher delegate = e -> called.set(true);
        SilentEventPublisher publisher = new SilentEventPublisher(delegate);

        // Act
        assertDoesNotThrow(() -> publisher.publish(event));

        // Assert
        assertTrue(called.get());
    }

    @Test
    @DisplayName("Should silently catch exception and log warning")
    void shouldSilentlyCatchExceptionAndLogWarning() {
        // Arrange
        TestEvent event = new TestEvent("test");
        EventPublisher delegate = e -> {
            throw new RuntimeException("Test exception");
        };
        SilentEventPublisher publisher = new SilentEventPublisher(delegate, true);

        // Act & Assert
        assertDoesNotThrow(() -> publisher.publish(event));
    }

    @Test
    @DisplayName("Should silently catch exception and log debug")
    void shouldSilentlyCatchExceptionAndLogDebug() {
        // Arrange
        TestEvent event = new TestEvent("test");
        EventPublisher delegate = e -> {
            throw new RuntimeException("Test exception");
        };
        SilentEventPublisher publisher = new SilentEventPublisher(delegate, false);

        // Act & Assert
        assertDoesNotThrow(() -> publisher.publish(event));
    }

    @Test
    @DisplayName("Should handle different exception types")
    void shouldHandleDifferentExceptionTypes() {
        // Arrange
        TestEvent event = new TestEvent("test");
        EventPublisher delegate = e -> {
            throw new EventPublisherException("Test");
        };
        SilentEventPublisher publisher = new SilentEventPublisher(delegate);

        // Act & Assert
        assertDoesNotThrow(() -> publisher.publish(event));
    }

    /**
     * Test event class.
     */
    private static final class TestEvent extends AbstractTraceableEvent {
        private final String data;

        TestEvent(String data) {
            super();
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @Override
        public String asJson() {
            return "{\"data\":\"" + data + "\",\"timestamp\":\"" + LocalDateTime.now() + "\"}";
        }
    }
}
