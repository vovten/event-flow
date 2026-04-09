package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.SendResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
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
        EventPublisher delegate = e -> CompletableFuture.completedFuture(List.of(SendResult.success("dest")));
        SilentEventPublisher publisher = new SilentEventPublisher(delegate);

        // Act
        CompletableFuture<List<SendResult>> future = publisher.publish(event);
        List<SendResult> results = future.join();

        // Assert
        assertThat(results).hasSize(1);
    }

    @Test
    @DisplayName("Should silently catch exception and log warning")
    void shouldSilentlyCatchExceptionAndLogWarning() {
        // Arrange
        TestEvent event = new TestEvent("test");
        EventPublisher delegate = e -> CompletableFuture.failedFuture(new RuntimeException("Test exception"));
        SilentEventPublisher publisher = new SilentEventPublisher(delegate, true);

        // Act & Assert
        CompletableFuture<List<SendResult>> future = publisher.publish(event);
        List<SendResult> results = future.join();

        assertDoesNotThrow(() -> results);
        assertThat(results).isEmpty();
    }

    @Test
    @DisplayName("Should silently catch exception and log debug")
    void shouldSilentlyCatchExceptionAndLogDebug() {
        // Arrange
        TestEvent event = new TestEvent("test");
        EventPublisher delegate = e -> CompletableFuture.failedFuture(new RuntimeException("Test exception"));
        SilentEventPublisher publisher = new SilentEventPublisher(delegate, false);

        // Act & Assert
        CompletableFuture<List<SendResult>> future = publisher.publish(event);
        List<SendResult> results = future.join();

        assertDoesNotThrow(() -> results);
        assertThat(results).isEmpty();
    }

    @Test
    @DisplayName("Should handle different exception types")
    void shouldHandleDifferentExceptionTypes() {
        // Arrange
        TestEvent event = new TestEvent("test");
        EventPublisher delegate = e -> CompletableFuture.failedFuture(new EventPublisherException("Test"));
        SilentEventPublisher publisher = new SilentEventPublisher(delegate);

        // Act & Assert
        CompletableFuture<List<SendResult>> future = publisher.publish(event);
        List<SendResult> results = future.join();

        assertDoesNotThrow(() -> results);
        assertThat(results).isEmpty();
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
