package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.event.AbstractTraceableEvent;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.transport.SendResult;
import com.github.vovten.eventflow.transport.SendResults;
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
        EventPublisher delegate = e -> CompletableFuture.completedFuture(SendResults.of(List.of(SendResult.success("dest"))));
        SilentEventPublisher publisher = new SilentEventPublisher(delegate);

        // Act
        CompletableFuture<SendResults> future = publisher.publish(event);
        SendResults results = future.join();

        // Assert
        assertThat(results.isAllSuccess()).isTrue();
        assertThat(results.getTotalCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should silently catch exception and log warning")
    void shouldSilentlyCatchExceptionAndLogWarning() {
        // Arrange
        TestEvent event = new TestEvent("test");
        EventPublisher delegate = e -> CompletableFuture.failedFuture(new RuntimeException("Test exception"));
        SilentEventPublisher publisher = new SilentEventPublisher(delegate, true);

        // Act
        CompletableFuture<SendResults> future = publisher.publish(event);
        SendResults results = future.join();

        // Assert
        assertThat(results.isAllFailure()).isTrue();
        assertThat(results.getFailedCount()).isEqualTo(1);
        assertThat(results.getFirstFailure()).isPresent();
        assertThat(results.getFirstFailure().get().error()).isInstanceOf(RuntimeException.class);
        assertThat(results.getFirstFailure().get().errorDetails()).isEqualTo("Test exception");
    }

    @Test
    @DisplayName("Should silently catch exception and log debug")
    void shouldSilentlyCatchExceptionAndLogDebug() {
        // Arrange
        TestEvent event = new TestEvent("test");
        EventPublisher delegate = e -> CompletableFuture.failedFuture(new RuntimeException("Test exception"));
        SilentEventPublisher publisher = new SilentEventPublisher(delegate, false);

        // Act
        CompletableFuture<SendResults> future = publisher.publish(event);
        SendResults results = future.join();

        // Assert
        assertThat(results.isAllFailure()).isTrue();
        assertThat(results.getFirstFailure().get().error()).isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("Should handle different exception types")
    void shouldHandleDifferentExceptionTypes() {
        // Arrange
        TestEvent event = new TestEvent("test");
        EventPublisher delegate = e -> CompletableFuture.failedFuture(new EventPublisherException("Test"));
        SilentEventPublisher publisher = new SilentEventPublisher(delegate);

        // Act
        CompletableFuture<SendResults> future = publisher.publish(event);
        SendResults results = future.join();

        // Assert
        assertThat(results.isAllFailure()).isTrue();
        assertThat(results.getFirstFailure().get().error()).isInstanceOf(EventPublisherException.class);
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
