package io.github.vovten.eventflow.channel;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.transport.OutTransport;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for EventChannel interface default methods.
 * @since 1.0.0
 */
@DisplayName("EventChannel Tests")
class EventChannelTest {

    private OutTransport transport1;
    private OutTransport transport2;
    private TestEventChannel channel;
    private TestEvent testEvent;

    private Logger channelLogger;
    private ListAppender<ILoggingEvent> listAppender;

    @BeforeEach
    void setUp() {
        transport1 = mock(OutTransport.class, "mock-transport-1");
        transport2 = mock(OutTransport.class, "mock-transport-2");
        when(transport1.name()).thenReturn("mock-transport-1");
        when(transport2.name()).thenReturn("mock-transport-2");
        channel = new TestEventChannel(List.of(transport1, transport2));
        testEvent = new TestEvent();

        channelLogger = (Logger) LoggerFactory.getLogger(AbstractEventChannel.class);
        listAppender = new ListAppender<>();
        listAppender.start();
        channelLogger.addAppender(listAppender);
    }

    @AfterEach
    void tearDown() {
        channelLogger.detachAppender(listAppender);
    }

    @Test
    @DisplayName("Should send event to all transports")
    void shouldSendEventToAllTransports() {
        when(transport1.send(testEvent)).thenReturn(CompletableFuture.completedFuture(SendResult.success("dest1")));
        when(transport2.send(testEvent)).thenReturn(CompletableFuture.completedFuture(SendResult.success("dest2")));

        CompletableFuture<SendResults> future = channel.send(testEvent);
        SendResults results = future.join();

        assertThat(results.isAllSuccess()).isTrue();
        assertThat(results.getTotalCount()).isEqualTo(2);
        verify(transport1).send(testEvent);
        verify(transport2).send(testEvent);
    }

    @Test
    @DisplayName("Should complete exceptionally when transport fails")
    void shouldCompleteExceptionallyWhenTransportFails() {
        when(transport1.send(testEvent)).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Transport failed")));
        when(transport2.send(testEvent)).thenReturn(CompletableFuture.completedFuture(SendResult.success("dest2")));

        CompletableFuture<SendResults> future = channel.send(testEvent);
        SendResults results = future.join();

        assertThat(results.isPartialSuccess()).isTrue();
        assertThat(results.getFailedCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should log warning when one transport fails")
    void shouldLogWarningWhenOneTransportFails() {
        // Arrange
        when(transport1.send(testEvent)).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection refused")));
        when(transport2.send(testEvent)).thenReturn(CompletableFuture.completedFuture(SendResult.success("dest2")));

        // Act
        CompletableFuture<SendResults> future = channel.send(testEvent);
        SendResults results = future.join();

        // Assert — partial success
        assertThat(results.isPartialSuccess()).isTrue();
        assertThat(results.getSuccessfulCount()).isEqualTo(1);
        assertThat(results.getFailedCount()).isEqualTo(1);

        // Assert — failure result contains transport name
        assertThat(results.getFirstFailure()).isPresent();
        assertThat(results.getFirstFailure().get().destination()).isEqualTo("mock-transport-1");
        assertThat(results.getFirstFailure().get().error()).isInstanceOf(RuntimeException.class);
        assertThat(results.getFirstFailure().get().errorDetails()).isEqualTo("Connection refused");

        // Assert — warning log was written with transport name
        List<ILoggingEvent> logs = listAppender.list;
        assertThat(logs).hasSize(1);
        ILoggingEvent logEvent = logs.get(0);
        assertThat(logEvent.getLevel()).isEqualTo(Level.WARN);
        assertThat(logEvent.getFormattedMessage()).contains("mock-transport-1");
        assertThat(logEvent.getFormattedMessage()).contains("TestEvent");
        assertThat(logEvent.getFormattedMessage()).contains("Connection refused");
        assertThat(logEvent.getThrowableProxy()).isNotNull();
    }

    @Test
    @DisplayName("Should fail when channel has no transports")
    void shouldFailWhenChannelHasNoTransports() {
        assertThatThrownBy(() -> new TestEventChannel(List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must have at least one transport")
                .hasMessageContaining("test-channel");
    }

    static class TestEventChannel extends AbstractEventChannel {
        TestEventChannel(List<OutTransport> transports) {
            super(transports);
        }

        @Override
        public String name() {
            return "test-channel";
        }
    }

    static class TestEvent extends AbstractTraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }
}
