package io.github.vovten.eventflow.publisher;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.event.TraceableEvent;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LoggingEventPublisherTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> listAppender;
    private Logger logger;

    @BeforeEach
    void setUp() {
        logger = (Logger) LoggerFactory.getLogger(LoggingEventPublisher.class);
        listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(listAppender);
    }

    @Test
    @DisplayName("Should throw exception when origin is null")
    void shouldThrowExceptionWhenOriginIsNull() {
        assertThatThrownBy(() -> new LoggingEventPublisher(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("origin must not be null");
    }

    @Test
    @DisplayName("Should pass envelope event through")
    void shouldPassEnvelopeEvent() {
        LoggingEventPublisher underTest = new LoggingEventPublisher(e -> {
            assertThat(e).isInstanceOf(Envelope.class);
            return CompletableFuture.completedFuture(SendResults.empty());
        });

        Envelope<String> envelope = Envelope.of("test-payload");

        underTest.publish(envelope).join();
    }

    @Test
    @DisplayName("Should handle TraceableEvent")
    void shouldHandleTraceableEvent() {
        LoggingEventPublisher underTest = new LoggingEventPublisher(e -> {
            assertThat(e).isInstanceOf(TestTraceableEvent.class);
            return CompletableFuture.completedFuture(SendResults.empty());
        });

        TestTraceableEvent event = new TestTraceableEvent();
        underTest.publish(event).join();
    }

    @Test
    @DisplayName("Should truncate long payload")
    void shouldTruncateLongPayload() {
        LoggingEventPublisher underTest = new LoggingEventPublisher(e -> {
            return CompletableFuture.completedFuture(SendResults.empty());
        });

        StringBuilder longStr = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            longStr.append("verylongword");
        }
        Envelope<String> envelope = Envelope.of(longStr.toString());

        underTest.publish(envelope).join();
    }

    @Test
    @DisplayName("Should not serialize large byte[] payload")
    void shouldNotSerializeLargeByteArray() {
        LoggingEventPublisher underTest = new LoggingEventPublisher(e -> {
            return CompletableFuture.completedFuture(SendResults.empty());
        }, 10);

        byte[] largeBytes = new byte[1000];
        Envelope<byte[]> envelope = Envelope.of(largeBytes);

        underTest.publish(envelope).join();
    }

    @Test
    @DisplayName("Should log published status when all sends succeed")
    void shouldLogPublishedStatusWhenAllSuccess() {
        LoggingEventPublisher underTest = new LoggingEventPublisher(e ->
                CompletableFuture.completedFuture(SendResults.of(List.of(
                        SendResult.success("ch1"),
                        SendResult.success("ch2")
                ))));

        Envelope<String> envelope = Envelope.of("test");
        underTest.publish(envelope).join();

        JsonNode log = captureSingleLog();
        assertThat(log.path("event").path("status").asText()).isEqualTo("published");
        assertThat(log.path("event").path("deliveredTo").isArray()).isTrue();
        assertThat(log.path("event").path("deliveredTo").get(0).asText()).isEqualTo("ch1");
    }

    @Test
    @DisplayName("Should log partial status when some sends fail")
    void shouldLogPartialStatusWhenSomeFail() {
        LoggingEventPublisher underTest = new LoggingEventPublisher(e ->
                CompletableFuture.completedFuture(SendResults.of(List.of(
                        SendResult.success("ch1"),
                        SendResult.failure("ch2", "connection lost")
                ))));

        Envelope<String> envelope = Envelope.of("test");
        underTest.publish(envelope).join();

        JsonNode log = captureSingleLog();
        assertThat(log.path("event").path("status").asText()).isEqualTo("partial");
        assertThat(log.path("event").path("deliveredTo").get(0).asText()).isEqualTo("ch1");
        assertThat(log.path("event").path("failedOn").get(0).asText()).contains("ch2", "connection lost");
    }

    @Test
    @DisplayName("Should log failed status when all sends fail")
    void shouldLogFailedStatusWhenAllFail() {
        LoggingEventPublisher underTest = new LoggingEventPublisher(e ->
                CompletableFuture.completedFuture(SendResults.of(List.of(
                        SendResult.failure("ch1", "timeout"),
                        SendResult.failure("ch2", "overflow")
                ))));

        Envelope<String> envelope = Envelope.of("test");
        underTest.publish(envelope).join();

        JsonNode log = captureSingleLog();
        assertThat(log.path("event").path("status").asText()).isEqualTo("failed");
        assertThat(log.path("event").path("failedOn").isArray()).isTrue();
    }

    @Test
    @DisplayName("Should log error info when future completes exceptionally")
    void shouldLogErrorInfoWhenException() {
        LoggingEventPublisher underTest = new LoggingEventPublisher(e ->
                CompletableFuture.failedFuture(new RuntimeException("broker down")));

        Envelope<String> envelope = Envelope.of("test");
        try {
            underTest.publish(envelope).join();
        } catch (Exception expected) {
            // future is failed by design
        }

        JsonNode log = captureSingleLog();
        assertThat(log.path("event").path("status").asText()).isEqualTo("failed");
        assertThat(log.path("event").path("error").path("message").asText()).isEqualTo("broker down");
        assertThat(log.path("event").path("error").path("type").asText()).isEqualTo("RuntimeException");
    }

    @Test
    @DisplayName("Should log @timestamp captured before send")
    void shouldIncludePreSendTimestamp() {
        LoggingEventPublisher underTest = new LoggingEventPublisher(e ->
                CompletableFuture.completedFuture(SendResults.empty()));

        Instant before = Instant.now();
        Envelope<String> envelope = Envelope.of("test");
        underTest.publish(envelope).join();
        Instant after = Instant.now();

        JsonNode log = captureSingleLog();
        Instant loggedTimestamp = Instant.parse(log.path("@timestamp").asText());
        assertThat(loggedTimestamp).isBetween(before, after);
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

    record TestTraceableEvent() implements TraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return TestTraceableEvent.class;
        }

        @Override
        public List<Class<? extends io.github.vovten.eventflow.channel.EventChannel>> channels() {
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