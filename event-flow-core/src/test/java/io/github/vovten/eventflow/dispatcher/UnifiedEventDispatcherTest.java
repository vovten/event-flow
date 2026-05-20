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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for UnifiedEventDispatcher.
 * @since 1.0.0
 */
@DisplayName("UnifiedEventDispatcher Tests")
class UnifiedEventDispatcherTest {

    private ExecutorService executorService;
    private EventHandlerRegistry handlerRegistry;
    private InTransport transport1;
    private InTransport transport2;
    private UnifiedEventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        handlerRegistry = mock(EventHandlerRegistry.class);
        transport1 = mock(InTransport.class);
        transport2 = mock(InTransport.class);
        when(transport1.name()).thenReturn("transport1");
        when(transport2.name()).thenReturn("transport2");
    }

    @Test
    @DisplayName("Should create dispatcher")
    void shouldCreateDispatcher() {
        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport1));

        assertNotNull(dispatcher);
    }

    @Test
    @DisplayName("Should start all transports")
    void shouldStartAllTransports() {
        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport1, transport2));
        dispatcher.start(dispatcher::dispatch);

        verify(transport1).start(any(Consumer.class));
        verify(transport2).start(any(Consumer.class));
    }

    @Test
    @DisplayName("Should stop all transports")
    void shouldStopAllTransports() {
        doAnswer(invocation -> {
            Consumer<Event> consumer = invocation.getArgument(0);
            return null;
        }).when(transport1).start(any(Consumer.class));

        doAnswer(invocation -> {
            Consumer<Event> consumer = invocation.getArgument(0);
            return null;
        }).when(transport2).start(any(Consumer.class));

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport1, transport2));
        dispatcher.start(dispatcher::dispatch);
        dispatcher.stop();

        verify(transport1).stop();
        verify(transport2).stop();
    }

    @Test
    @DisplayName("Should log warning when transport throws on stop")
    void shouldLogWarningWhenTransportThrowsOnStop() {
        doAnswer(invocation -> {
            Consumer<Event> consumer = invocation.getArgument(0);
            return null;
        }).when(transport1).start(any(Consumer.class));
        doThrow(new RuntimeException("Stop failed")).when(transport1).stop();

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport1));
        dispatcher.start(dispatcher::dispatch);

        assertDoesNotThrow(() -> dispatcher.stop());
    }

    @Test
    @DisplayName("Should do nothing when no handlers")
    void shouldDoNothingWhenNoHandlers() {
        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport1));
        when(handlerRegistry.getHandlers(any())).thenReturn(List.of());

        TestEvent event = new TestEvent();
        dispatcher.dispatch(event);

        verify(handlerRegistry).getHandlers(event);
    }

    @Test
    @DisplayName("Should execute handler asynchronously")
    void shouldExecuteHandlerAsynchronously() {
        TestEventSubscriber subscriber = new TestEventSubscriber();
        when(handlerRegistry.getHandlers(any())).thenReturn(List.of(subscriber));

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport1));
        TestEvent event = new TestEvent();
        dispatcher.dispatch(event);

        await().atMost(Duration.ofSeconds(2)).until(subscriber::wasCalled);
    }

    @Test
    @DisplayName("Should delegate register to registry")
    void shouldDelegateRegisterToRegistry() {
        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport1));
        Object handler = new Object();
        when(handlerRegistry.isRegistered(handler)).thenReturn(false);

        dispatcher.register(handler);

        verify(handlerRegistry).register(handler);
    }

    @Test
    @DisplayName("Should not register if already registered")
    void shouldNotRegisterIfAlreadyRegistered() {
        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport1));
        Object handler = new Object();
        when(handlerRegistry.isRegistered(handler)).thenReturn(true);

        dispatcher.register(handler);

        verify(handlerRegistry, never()).register(handler);
    }

    @Test
    @DisplayName("Should delegate isRegistered to registry")
    void shouldDelegateIsRegisteredToRegistry() {
        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport1));
        Object handler = new Object();
        when(handlerRegistry.isRegistered(handler)).thenReturn(true);

        boolean result = dispatcher.isRegistered(handler);

        assertTrue(result);
    }

    @Test
    @DisplayName("Should release semaphore when handler throws exception")
    void shouldReleaseSemaphoreWhenHandlerThrowsException() {
        Semaphore semaphore = new Semaphore(1);

        EventHandler failingHandler = mock(EventHandler.class);
        doThrow(new RuntimeException("Handler failed!")).when(failingHandler).onEvent(any());

        when(handlerRegistry.getHandlers(any())).thenReturn(List.of(failingHandler));

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport1), semaphore);

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
