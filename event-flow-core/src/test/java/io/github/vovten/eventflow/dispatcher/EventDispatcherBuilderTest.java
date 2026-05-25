package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.registry.EventHandlerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link EventDispatcherBuilder}.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
@DisplayName("EventDispatcherBuilder Tests")
class EventDispatcherBuilderTest {

    private ExecutorService executorService;
    private EventHandlerRegistry handlerRegistry;
    private Map<Class<?>, List<EventHandler>> handlerMap;

    @BeforeEach
    void setUp() {
        executorService = Executors.newSingleThreadExecutor();
        handlerMap = new HashMap<>();
        handlerRegistry = new MapBasedHandlerRegistry(handlerMap);
    }

    @Test
    @DisplayName("Should build dispatcher with required parameters")
    void shouldBuildDispatcherWithRequiredParameters() {
        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .build();

        assertNotNull(dispatcher);
        assertInstanceOf(UnifiedEventDispatcher.class, dispatcher);
    }

    @Test
    @DisplayName("Should build and log dispatcher")
    void shouldBuildAndLogDispatcher() {
        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .buildAndLog();

        assertNotNull(dispatcher);
    }

    @Test
    @DisplayName("Should throw exception when executor is not set")
    void shouldThrowExceptionWhenExecutorIsNotSet() {
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> EventDispatcherBuilder.create()
                        .handlerRegistry(handlerRegistry)
                        .transports(List.of())
                        .build()
        );
        assertEquals("ExecutorService must be configured", exception.getMessage());
    }

    @Test
    @DisplayName("Should throw exception when handler registry is not set")
    void shouldThrowExceptionWhenHandlerRegistryIsNotSet() {
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> EventDispatcherBuilder.create()
                        .executor(executorService)
                        .transports(List.of())
                        .build()
        );
        assertEquals("EventHandlerRegistry must be configured", exception.getMessage());
    }

    @Test
    @DisplayName("Should build dispatcher with empty transports list")
    void shouldBuildDispatcherWithEmptyTransportsList() {
        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .build();

        assertNotNull(dispatcher);
    }

    @Test
    @DisplayName("Should build dispatcher with idempotent processing enabled (default settings)")
    void shouldBuildDispatcherWithIdempotentEnabled() {
        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .idempotent()
                .build();

        assertNotNull(dispatcher);
        assertInstanceOf(IdempotentEventDispatcher.class, dispatcher);
    }

    @Test
    @DisplayName("Should build dispatcher with idempotent processing (custom settings)")
    void shouldBuildDispatcherWithIdempotentCustomSettings() {
        Duration ttl = Duration.ofMinutes(30);
        long maxSize = 50_000L;
        boolean warnOnDuplicate = false;

        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .idempotent(ttl, maxSize, warnOnDuplicate)
                .build();

        assertNotNull(dispatcher);
        assertInstanceOf(IdempotentEventDispatcher.class, dispatcher);
    }

    @Test
    @DisplayName("Should apply custom decorator")
    void shouldApplyCustomDecorator() {
        AtomicBoolean decoratorApplied = new AtomicBoolean(false);

        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .withDecorator(d -> {
                    decoratorApplied.set(true);
                    return d;
                })
                .build();

        assertTrue(decoratorApplied.get());
    }

    @Test
    @DisplayName("Should apply multiple custom decorators")
    void shouldApplyMultipleCustomDecorators() {
        AtomicInteger decoratorCount = new AtomicInteger(0);

        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .withDecorator(d -> {
                    decoratorCount.incrementAndGet();
                    return d;
                })
                .withDecorator(d -> {
                    decoratorCount.incrementAndGet();
                    return d;
                })
                .build();

        assertEquals(2, decoratorCount.get());
    }

    @Test
    @DisplayName("Should apply idempotent decorator after custom decorators")
    void shouldApplyIdempotentAfterCustomDecorators() {
        AtomicInteger decoratorOrder = new AtomicInteger(0);
        final int[] order = new int[2];

        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .withDecorator(d -> {
                    order[0] = decoratorOrder.incrementAndGet();
                    return d;
                })
                .idempotent()
                .build();

        // Custom decorator should be applied first (order[0] = 1)
        // Idempotent decorator should be applied second (innermost to outermost)
        assertEquals(1, order[0]);
        assertInstanceOf(IdempotentEventDispatcher.class, dispatcher);
    }

    @Test
    @DisplayName("Should add transports via addTransports(List)")
    void shouldAddTransportsViaList() {
        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .addTransports(List.of())
                .build();

        assertNotNull(dispatcher);
    }

    @Test
    @DisplayName("Should add transports via addTransports(varargs)")
    void shouldAddTransportsViaVarargs() {
        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .addTransports()
                .build();

        assertNotNull(dispatcher);
    }

    @Test
    @DisplayName("Should dispatch event with idempotent processing")
    void shouldDispatchEventWithIdempotentProcessing() throws Exception {
        TestEvent event = new TestEvent("test-data");
        TestEventHandler handler = new TestEventHandler();
        handlerMap.put(TestEvent.class, List.of(handler));

        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .idempotent(Duration.ofMinutes(1), 100, false)
                .build();

        dispatcher.start(dispatcher::dispatch);
        try {
            dispatcher.dispatch(event);
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);

            assertTrue(handler.called);
            assertEquals(event, handler.receivedEvent);
        } finally {
            dispatcher.stop();
        }
    }

    @Test
    @DisplayName("Should prevent duplicate event processing with idempotent")
    void shouldPreventDuplicateEventProcessing() throws Exception {
        TestEvent event = new TestEvent("test-data");
        AtomicInteger handlerCallCount = new AtomicInteger(0);

        EventHandler handler = e -> handlerCallCount.incrementAndGet();
        handlerMap.put(TestEvent.class, List.of(handler));

        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .idempotent(Duration.ofMinutes(1), 100, false)
                .build();

        dispatcher.start(dispatcher::dispatch);
        try {
            dispatcher.dispatch(event).get(); // Wait for first dispatch to complete
            dispatcher.dispatch(event).get(); // Duplicate - should be filtered
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);

            assertEquals(1, handlerCallCount.get());
        } finally {
            dispatcher.stop();
        }
    }

    @Test
    @DisplayName("Should allow non-TraceableEvent through idempotent dispatcher")
    void shouldAllowNonTraceableEventThroughIdempotent() throws Exception {
        NonTraceableTestEvent event = new NonTraceableTestEvent();
        AtomicInteger handlerCallCount = new AtomicInteger(0);

        EventHandler handler = e -> handlerCallCount.incrementAndGet();
        handlerMap.put(NonTraceableTestEvent.class, List.of(handler));

        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .idempotent(Duration.ofMinutes(1), 100, false)
                .build();

        dispatcher.start(dispatcher::dispatch);
        try {
            dispatcher.dispatch(event);
            dispatcher.dispatch(event); // Same event, but not traceable
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);

            assertEquals(2, handlerCallCount.get());
        } finally {
            dispatcher.stop();
        }
    }

    @Test
    @DisplayName("Chain should build dispatcher with required parameters")
    void chainShouldBuildDispatcherWithRequiredParameters() {
        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .chain()
                .build();

        assertNotNull(dispatcher);
        assertInstanceOf(UnifiedEventDispatcher.class, dispatcher);
    }

    @Test
    @DisplayName("Chain should apply decorators in added order")
    void chainShouldApplyDecoratorsInAddedOrder() {
        AtomicBoolean firstApplied = new AtomicBoolean(false);
        AtomicBoolean secondApplied = new AtomicBoolean(false);

        EventDispatcherBuilder.DecoratorFunction first = d -> {
            firstApplied.set(true);
            assertInstanceOf(UnifiedEventDispatcher.class, d);
            return d;
        };
        EventDispatcherBuilder.DecoratorFunction second = d -> {
            secondApplied.set(true);
            assertTrue(firstApplied.get(), "First decorator should have been applied before second");
            return d;
        };

        EventDispatcher dispatcher = EventDispatcherBuilder.create()
                .executor(executorService)
                .handlerRegistry(handlerRegistry)
                .transports(List.of())
                .chain()
                .add(first)
                .add(second)
                .build();

        assertNotNull(dispatcher);
        assertTrue(firstApplied.get());
        assertTrue(secondApplied.get());
    }

    @Test
    @DisplayName("Chain should detect missing executor")
    void chainShouldDetectMissingExecutor() {
        assertThrows(IllegalStateException.class, () ->
                EventDispatcherBuilder.create()
                        .handlerRegistry(handlerRegistry)
                        .transports(List.of())
                        .chain()
                        .build());
    }

    @Test
    @DisplayName("Chain should detect missing handler registry")
    void chainShouldDetectMissingHandlerRegistry() {
        assertThrows(IllegalStateException.class, () ->
                EventDispatcherBuilder.create()
                        .executor(executorService)
                        .transports(List.of())
                        .chain()
                        .build());
    }

    /**
     * Test event.
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
            return "{\"data\":\"" + data + "\"}";
        }
    }

    /**
     * Test event handler.
     */
    private static final class TestEventHandler implements EventHandler {
        private boolean called = false;
        private TestEvent receivedEvent;

        @Override
        public void onEvent(Event event) {
            this.called = true;
            this.receivedEvent = (TestEvent) event;
        }
    }

    /**
     * Non-traceable event for idempotent filter tests.
     */
    private static final class NonTraceableTestEvent implements Event {
        @Override
        public Class<? extends Event> type() {
            return NonTraceableTestEvent.class;
        }
    }
}
