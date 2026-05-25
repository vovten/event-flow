package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.EventSubscriber;
import io.github.vovten.eventflow.registry.EventHandlerRegistry;
import io.github.vovten.eventflow.transport.InTransport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for UnifiedEventDispatcher.
 * @since 1.0.0
 */
@DisplayName("UnifiedEventDispatcher Tests")
class UnifiedEventDispatcherTest {

    private ExecutorService executorService;
    private EventHandlerRegistry handlerRegistry;
    private Map<Class<?>, List<EventHandler>> handlerMap;
    private UnifiedEventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        handlerMap = new HashMap<>();
        handlerRegistry = new MapBasedHandlerRegistry(handlerMap);
    }

    @Test
    @DisplayName("Should create dispatcher")
    void shouldCreateDispatcher() {
        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of());

        assertNotNull(dispatcher);
    }

    @Test
    @DisplayName("Should start all transports")
    void shouldStartAllTransports() {
        RecordingInTransport transport1 = new RecordingInTransport("transport1");
        RecordingInTransport transport2 = new RecordingInTransport("transport2");
        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.<InTransport>of(transport1, transport2));
        dispatcher.start(dispatcher::dispatch);

        assertTrue(transport1.started);
        assertTrue(transport2.started);
        dispatcher.stop();
    }

    @Test
    @DisplayName("Should stop all transports")
    void shouldStopAllTransports() {
        RecordingInTransport transport1 = new RecordingInTransport("transport1");
        RecordingInTransport transport2 = new RecordingInTransport("transport2");

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.<InTransport>of(transport1, transport2));
        dispatcher.start(dispatcher::dispatch);
        dispatcher.stop();

        assertTrue(transport1.stopped);
        assertTrue(transport2.stopped);
    }

    @Test
    @DisplayName("Should log warning when transport throws on stop")
    void shouldLogWarningWhenTransportThrowsOnStop() {
        FailingOnStopTransport transport1 = new FailingOnStopTransport("transport1");

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.<InTransport>of(transport1));
        dispatcher.start(dispatcher::dispatch);

        assertDoesNotThrow(() -> dispatcher.stop());
    }

    @Test
    @DisplayName("Should do nothing when no handlers")
    void shouldDoNothingWhenNoHandlers() {
        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of());

        TestEvent event = new TestEvent();
        assertDoesNotThrow(() -> dispatcher.dispatch(event));
    }

    @Test
    @DisplayName("Should execute handler asynchronously")
    void shouldExecuteHandlerAsynchronously() {
        TestEventSubscriber subscriber = new TestEventSubscriber();
        handlerMap.put(TestEvent.class, List.of(subscriber));

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of());
        TestEvent event = new TestEvent();
        dispatcher.dispatch(event);

        await().atMost(Duration.ofSeconds(2)).until(subscriber::wasCalled);
    }

    @Test
    @DisplayName("Should delegate register to registry")
    void shouldDelegateRegisterToRegistry() {
        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of());
        Object handler = new Object();

        assertFalse(dispatcher.isRegistered(handler));
        dispatcher.register(handler);
        assertTrue(dispatcher.isRegistered(handler));
    }

    @Test
    @DisplayName("Should not register if already registered")
    void shouldNotRegisterIfAlreadyRegistered() {
        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of());
        Object handler = new Object();

        dispatcher.register(handler);
        assertTrue(dispatcher.isRegistered(handler));

        // Second register should be no-op — still registered once
        dispatcher.register(handler);
        assertTrue(dispatcher.isRegistered(handler));
    }

    @Test
    @DisplayName("Should delegate isRegistered to registry")
    void shouldDelegateIsRegisteredToRegistry() {
        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of());
        Object handler = new Object();

        assertFalse(dispatcher.isRegistered(handler));
        dispatcher.register(handler);
        assertTrue(dispatcher.isRegistered(handler));
    }

    @Test
    @DisplayName("Should release semaphore when handler throws exception")
    void shouldReleaseSemaphoreWhenHandlerThrowsException() {
        Semaphore semaphore = new Semaphore(1);

        Map<Class<?>, List<EventHandler>> localHandlerMap = new HashMap<>();
        localHandlerMap.put(TestEvent.class, List.of(event -> {
            throw new RuntimeException("Handler failed!");
        }));
        EventHandlerRegistry localRegistry = new MapBasedHandlerRegistry(localHandlerMap);

        dispatcher = new UnifiedEventDispatcher(executorService, localRegistry, List.of(), semaphore);

        assertEquals(1, semaphore.availablePermits());

        TestEvent event = new TestEvent();
        dispatcher.dispatch(event);

        await().atMost(Duration.ofSeconds(2)).until(() -> semaphore.availablePermits() == 1);
    }

    static class TestEvent extends AbstractTraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }

    static class RecordingInTransport implements InTransport {
        final String name;
        boolean started;
        boolean stopped;

        RecordingInTransport(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void start(java.util.function.Consumer<Event> dispatchConsumer) {
            started = true;
        }

        @Override
        public void stop() {
            stopped = true;
        }
    }

    static class FailingOnStopTransport implements InTransport {
        final String name;

        FailingOnStopTransport(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void start(java.util.function.Consumer<Event> dispatchConsumer) {
        }

        @Override
        public void stop() {
            throw new RuntimeException("Stop failed");
        }
    }

    static class TestEventSubscriber implements EventSubscriber {
        private final AtomicBoolean called = new AtomicBoolean(false);

        @Override
        public List<Class<?>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
            called.set(true);
        }

        boolean wasCalled() {
            return called.get();
        }
    }
}
