package io.github.vovten.eventflow.dispatcher;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.registry.EventHandlerRegistry;
import io.github.vovten.eventflow.transport.incoming.LocalQueueInTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Integration tests for EventDispatcherBuilder with all decorators.
 * Verifies that LoggingEventDispatcher properly logs events including duplicates
 * when IdempotentEventDispatcher is inside the logging decorator.
 *
 * @since 1.1.0
 */
@DisplayName("Full Dispatcher Stack Integration Tests")
class FullDispatcherStackTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> listAppender;
    private Logger logger;
    private EventHandlerRegistry handlerRegistry;

    @BeforeEach
    void setUp() {
        logger = (Logger) LoggerFactory.getLogger(LoggingEventDispatcher.class);
        listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);
        handlerRegistry = mock(EventHandlerRegistry.class);
        MDC.clear();
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(listAppender);
        MDC.clear();
    }

    @Test
    @DisplayName("Should log duplicate event when IdempotentEventDispatcher is inside LoggingEventDispatcher")
    void shouldLogDuplicateEventWithFullStack() throws Exception {
        TestEvent event = new TestEvent("test-data");
        TestEventHandler handler = new TestEventHandler();
        CountDownLatch latch = new CountDownLatch(1);
        handler.latch = latch;
        when(handlerRegistry.getHandlers(event)).thenReturn(List.of(handler));

        // Build dispatcher with idempotent AND logging
        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(Executors.newSingleThreadExecutor())
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .idempotent(Duration.ofMinutes(1), 100, true)
                .loggable()
                .build();

        dispatcher.start(dispatcher::dispatch);
        try {
            // First dispatch - should be handled
            dispatcher.dispatch(event).join();
            latch.await(1, TimeUnit.SECONDS);

            // Reset handler and latch for second dispatch
            handler.called = false;
            latch = new CountDownLatch(1);
            handler.latch = latch;

            // Second dispatch - should be duplicate and skipped
            dispatcher.dispatch(event).join();
            latch.await(1, TimeUnit.SECONDS);

            // Handler should NOT be called for duplicate
            assertThat(handler.called).isFalse();

            // Find the log entry for duplicate (should have skipReason = duplicate event)
            String duplicateLog = listAppender.list.stream()
                    .map(e -> e.getFormattedMessage())
                    .filter(msg -> msg.contains("duplicate event"))
                    .findFirst()
                    .orElseThrow();

            JsonNode log = MAPPER.readTree(duplicateLog);
            assertThat(log.path("event").path("status").asText()).isEqualTo("skipped");
            assertThat(log.path("event").path("statusDesc").asText()).isEqualTo("duplicate event");
            assertThat(log.path("event").path("handlers").isEmpty()).isTrue();
        } finally {
            dispatcher.stop();
        }
    }

    @Test
    @DisplayName("Should log no handlers found when handlers are not registered")
    void shouldLogNoHandlersFoundWithFullStack() throws Exception {
        TestEvent event = new TestEvent("test-data");
        when(handlerRegistry.getHandlers(event)).thenReturn(List.of());

        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(Executors.newSingleThreadExecutor())
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .idempotent(Duration.ofMinutes(1), 100, true)
                .loggable()
                .build();

        dispatcher.start(dispatcher::dispatch);
        try {
            dispatcher.dispatch(event).join();

            // Give time for async logging
            Thread.sleep(100);

            // Find the log entry for skipped (should have skipReason = no handlers found)
            String skippedLog = listAppender.list.stream()
                    .map(e -> e.getFormattedMessage())
                    .filter(msg -> msg.contains("no handlers found"))
                    .findFirst()
                    .orElseThrow();

            JsonNode log = MAPPER.readTree(skippedLog);
            assertThat(log.path("event").path("status").asText()).isEqualTo("skipped");
            assertThat(log.path("event").path("statusDesc").asText()).isEqualTo("no handlers found");
            assertThat(log.path("event").path("handlers").isEmpty()).isTrue();
        } finally {
            dispatcher.stop();
        }
    }

    @Test
    @DisplayName("Should log handled event when handler succeeds")
    void shouldLogHandledEventWithFullStack() throws Exception {
        TestEvent event = new TestEvent("test-data");
        TestEventHandler handler = new TestEventHandler();
        CountDownLatch latch = new CountDownLatch(1);
        handler.latch = latch;
        when(handlerRegistry.getHandlers(event)).thenReturn(List.of(handler));

        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(Executors.newSingleThreadExecutor())
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .idempotent(Duration.ofMinutes(1), 100, true)
                .loggable()
                .build();

        dispatcher.start(dispatcher::dispatch);
        try {
            dispatcher.dispatch(event).join();
            latch.await(1, TimeUnit.SECONDS);

            assertThat(handler.called).isTrue();

            // Find the log entry for handled
            String handledLog = listAppender.list.stream()
                    .map(e -> e.getFormattedMessage())
                    .filter(msg -> msg.contains("\"handled\""))
                    .findFirst()
                    .orElseThrow();

            JsonNode log = MAPPER.readTree(handledLog);
            assertThat(log.path("event").path("status").asText()).isEqualTo("handled");
            assertThat(log.path("event").path("handlers").isEmpty()).isFalse();
        } finally {
            dispatcher.stop();
        }
    }

    @Test
    @DisplayName("Should include deliveredFrom in log when event comes from transport")
    void shouldIncludeDeliveredFromWithFullStack() throws Exception {
        TestEvent event = new TestEvent("test-data");
        TestEventHandler handler = new TestEventHandler();
        CountDownLatch latch = new CountDownLatch(1);
        handler.latch = latch;
        when(handlerRegistry.getHandlers(event)).thenReturn(List.of(handler));

        // Use LocalQueueInTransport to set deliveredFrom in MDC
        LinkedBlockingDeque<Event> queue = new LinkedBlockingDeque<>();
        LocalQueueInTransport transport = new LocalQueueInTransport(queue);

        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(Executors.newSingleThreadExecutor())
                .handlerRegistry(handlerRegistry)
                .transports(List.of(transport))
                .idempotent(Duration.ofMinutes(1), 100, true)
                .loggable()
                .build();

        // Start transport to consume from queue
        transport.start(dispatcher::dispatch);
        dispatcher.start(dispatcher::dispatch);

        try {
            // Give time for transport to start
            Thread.sleep(100);

            // Publish event to transport queue - this goes through transport
            queue.put(event);

            latch.await(1, TimeUnit.SECONDS);
            Thread.sleep(100);

            // Find the log entry with deliveredFrom
            String deliveredLog = listAppender.list.stream()
                    .map(e -> e.getFormattedMessage())
                    .filter(msg -> msg.contains("deliveredFrom"))
                    .filter(msg -> msg.contains("\"handled\""))
                    .findFirst()
                    .orElseThrow();

            JsonNode log = MAPPER.readTree(deliveredLog);
            assertThat(log.path("deliveredFrom").asText()).isEqualTo("local-queue");
        } finally {
            dispatcher.stop();
        }
    }

    private static class TestEvent extends AbstractTraceableEvent {
        private final String data;

        TestEvent(String data) {
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        public String getData() {
            return data;
        }
    }

    private static final class TestEventHandler implements EventHandler {
        boolean called;
        Event receivedEvent;
        CountDownLatch latch;

        @Override
        public void onEvent(Event event) {
            called = true;
            receivedEvent = event;
            if (latch != null) {
                latch.countDown();
            }
        }
    }
}
