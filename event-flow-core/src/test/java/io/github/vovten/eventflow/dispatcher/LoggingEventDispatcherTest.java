package io.github.vovten.eventflow.dispatcher;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LoggingEventDispatcherTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> listAppender;

    private Logger logger;

    @BeforeEach
    void setUp() {
        logger = (Logger) LoggerFactory.getLogger(LoggingEventDispatcher.class);
        listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(listAppender);
        MDC.clear();
    }

    @Test
    @DisplayName("Should log handled status when all handlers succeed")
    void shouldLogHandledStatusWhenAllSuccess() {
        LoggingEventDispatcher underTest = new LoggingEventDispatcher(dispatcherThatReturns(
                HandlerResults.of(List.of(
                        HandlerResult.success("h1"),
                        HandlerResult.success("h2")))));

        Envelope<String> envelope = Envelope.of("test");
        underTest.dispatch(envelope).join();

        JsonNode log = captureSingleLog();
        assertThat(log.path("event").path("status").asText()).isEqualTo("handled");
        assertThat(log.path("event").path("handlers").get(0).asText()).isEqualTo("h1");
        assertThat(log.path("event").path("handlers").get(1).asText()).isEqualTo("h2");
    }

    @Test
    @DisplayName("Should log partial status when some handlers fail")
    void shouldLogPartialStatusWhenSomeFail() {
        LoggingEventDispatcher underTest = new LoggingEventDispatcher(dispatcherThatReturns(
                HandlerResults.of(List.of(
                        HandlerResult.success("h1"),
                        HandlerResult.failure("h2", "processing error")))));

        Envelope<String> envelope = Envelope.of("test");
        underTest.dispatch(envelope).join();

        JsonNode log = captureSingleLog();
        assertThat(log.path("event").path("status").asText()).isEqualTo("partial");
        assertThat(log.path("event").path("failedHandlers").get(0).asText())
                .contains("h2", "processing error");
        assertThat(log.path("event").path("failedCount").asInt()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should log failed status when all handlers fail")
    void shouldLogFailedStatusWhenAllFail() {
        LoggingEventDispatcher underTest = new LoggingEventDispatcher(dispatcherThatReturns(
                HandlerResults.of(List.of(
                        HandlerResult.failure("h1", "timeout"),
                        HandlerResult.failure("h2", "error")))));

        Envelope<String> envelope = Envelope.of("test");
        underTest.dispatch(envelope).join();

        JsonNode log = captureSingleLog();
        assertThat(log.path("event").path("status").asText()).isEqualTo("failed");
        assertThat(log.path("event").path("failedCount").asInt()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should include occurredAt and processId for TraceableEvent")
    void shouldIncludeEventOccurredAt() {
        LoggingEventDispatcher underTest = new LoggingEventDispatcher(dispatcherThatReturns(
                HandlerResults.empty()));

        TestTraceableEvent event = new TestTraceableEvent();
        underTest.dispatch(event).join();

        JsonNode log = captureSingleLog();
        assertThat(log.path("event").path("occurredAt").isMissingNode()).isFalse();
        assertThat(log.path("event").path("processId").isMissingNode()).isFalse();
    }

    @Test
    @DisplayName("Should handle byte[] payload without serialization error")
    void shouldHandleByteArrayPayload() {
        LoggingEventDispatcher underTest = new LoggingEventDispatcher(
                dispatcherThatReturns(HandlerResults.empty()));

        byte[] bytes = new byte[1000];
        Envelope<byte[]> envelope = Envelope.of(bytes);

        underTest.dispatch(envelope).join();
    }

    @Test
    @DisplayName("Should truncate large JSON object payload with data fallback")
    void shouldTruncateLargeJsonObject() {
        LoggingEventDispatcher underTest = new LoggingEventDispatcher(
                dispatcherThatReturns(HandlerResults.empty()), 100);

        Map<String, String> largeMap = new LinkedHashMap<>();
        for (int i = 0; i < 30; i++) {
            largeMap.put("key" + i, "value" + i);
        }
        Envelope<Map<String, String>> envelope = Envelope.of(largeMap);

        underTest.dispatch(envelope).join();

        JsonNode log = captureSingleLog();
        assertThat(log.path("event").path("payload").path("_truncated").asBoolean()).isTrue();
        assertThat(log.path("event").path("payload").path("data").asText()).isNotEmpty();
    }

    @Test
    @DisplayName("Should capture MDC context before async dispatch")
    void shouldCaptureMdcContext() {
        MDC.put("traceId", "test-trace-123");
        MDC.put("spanId", "test-span-456");

        LoggingEventDispatcher underTest = new LoggingEventDispatcher(dispatcherThatReturns(
                HandlerResults.empty()));

        Envelope<String> envelope = Envelope.of("test");
        underTest.dispatch(envelope).join();

        JsonNode log = captureSingleLog();
        assertThat(log.path("traceId").asText()).isEqualTo("test-trace-123");
        assertThat(log.path("spanId").asText()).isEqualTo("test-span-456");
    }

    private static EventDispatcher dispatcherThatReturns(HandlerResults results) {
        EventDispatcher mock = mock(EventDispatcher.class);
        when(mock.dispatch(null)).thenReturn(CompletableFuture.completedFuture(results));
        when(mock.dispatch(any())).thenReturn(CompletableFuture.completedFuture(results));
        return mock;
    }

    private JsonNode captureSingleLog() {
        assertThat(listAppender.list).isNotEmpty();
        String json = listAppender.list.get(0).getFormattedMessage();
        try {
            return MAPPER.readTree(json);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse log JSON: " + json, e);
        }
    }

    private record TestTraceableEvent() implements TraceableEvent {

        @Override
        public Class<? extends Event> type() {
            return TestTraceableEvent.class;
        }

        @Override
        public List<Class<? extends EventChannel>> channels() {
            return List.of(InternalEventChannel.class);
        }

        @Override
        public UUID eventId() {
            return UUID.randomUUID();
        }

        @Override
        public UUID processId() {
            return UUID.randomUUID();
        }

        @Override
        public Instant occurredAt() {
            return Instant.now();
        }
    }
}
