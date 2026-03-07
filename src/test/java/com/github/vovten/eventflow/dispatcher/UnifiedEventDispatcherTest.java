package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.registry.EventListenerRegistry;
import com.github.vovten.eventflow.transport.IncomingEventTransport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for UnifiedEventDispatcher.
 */
class UnifiedEventDispatcherTest {

    private ExecutorService executorService;
    private EventListenerRegistry listenerRegistry;
    private IncomingEventTransport transport1;
    private IncomingEventTransport transport2;
    private UnifiedEventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        listenerRegistry = mock(EventListenerRegistry.class);
        transport1 = mock(IncomingEventTransport.class);
        transport2 = mock(IncomingEventTransport.class);
        when(transport1.name()).thenReturn("transport1");
        when(transport2.name()).thenReturn("transport2");
    }

    @Test
    void testConstructor() {
        dispatcher = new UnifiedEventDispatcher(executorService, listenerRegistry, List.of(transport1));

        assertNotNull(dispatcher);
    }

    @Test
    void testStart_StartsAllTransports() {
        dispatcher = new UnifiedEventDispatcher(executorService, listenerRegistry, List.of(transport1, transport2));

        dispatcher.start();

        verify(transport1).start(any(Consumer.class));
        verify(transport2).start(any(Consumer.class));
    }

    @Test
    void testStart_AlreadyStarted_LogsWarning() {
        dispatcher = new UnifiedEventDispatcher(executorService, listenerRegistry, List.of(transport1));
        dispatcher.start();

        assertDoesNotThrow(() -> dispatcher.start());
    }

    @Test
    void testStop_StopsAllTransports() {
        doAnswer(invocation -> {
            Consumer<Event> consumer = invocation.getArgument(0);
            return null;
        }).when(transport1).start(any(Consumer.class));
        doAnswer(invocation -> {
            Consumer<Event> consumer = invocation.getArgument(0);
            return null;
        }).when(transport2).start(any(Consumer.class));

        dispatcher = new UnifiedEventDispatcher(executorService, listenerRegistry, List.of(transport1, transport2));
        dispatcher.start();
        dispatcher.stop();

        verify(transport1).stop();
        verify(transport2).stop();
    }

    @Test
    void testStop_NotStarted_DoesNothing() {
        dispatcher = new UnifiedEventDispatcher(executorService, listenerRegistry, List.of(transport1));

        assertDoesNotThrow(() -> dispatcher.stop());
        verify(transport1, never()).stop();
    }

    @Test
    void testStop_TransportThrowsException_LogsWarning() {
        doAnswer(invocation -> {
            Consumer<Event> consumer = invocation.getArgument(0);
            return null;
        }).when(transport1).start(any(Consumer.class));
        doThrow(new RuntimeException("Stop failed")).when(transport1).stop();

        dispatcher = new UnifiedEventDispatcher(executorService, listenerRegistry, List.of(transport1));
        dispatcher.start();

        assertDoesNotThrow(() -> dispatcher.stop());
    }

    @Test
    void testDispatch_NoListeners_NoAction() {
        dispatcher = new UnifiedEventDispatcher(executorService, listenerRegistry, List.of(transport1));
        when(listenerRegistry.getListeners(any())).thenReturn(List.of());

        TestEvent event = new TestEvent();
        dispatcher.dispatch(event);

        verify(listenerRegistry).getListeners(event);
    }

    @Test
    void testDispatch_WithListeners_ExecutesAsync() throws InterruptedException {
        TestEventListener listener = new TestEventListener();
        when(listenerRegistry.getListeners(any())).thenReturn(List.of(listener));

        dispatcher = new UnifiedEventDispatcher(executorService, listenerRegistry, List.of(transport1));
        TestEvent event = new TestEvent();
        dispatcher.dispatch(event);

        Thread.sleep(100);
        assertTrue(listener.wasCalled());
    }

    @Test
    void testRegister_DelegatesToRegistry() {
        dispatcher = new UnifiedEventDispatcher(executorService, listenerRegistry, List.of(transport1));
        Object listener = new Object();
        when(listenerRegistry.isRegistered(listener)).thenReturn(false);

        dispatcher.register(listener);

        verify(listenerRegistry).register(listener);
    }

    @Test
    void testRegister_AlreadyRegistered_DoesNotRegisterAgain() {
        dispatcher = new UnifiedEventDispatcher(executorService, listenerRegistry, List.of(transport1));
        Object listener = new Object();
        when(listenerRegistry.isRegistered(listener)).thenReturn(true);

        dispatcher.register(listener);

        verify(listenerRegistry, never()).register(any());
    }

    @Test
    void testIsRegistered_DelegatesToRegistry() {
        dispatcher = new UnifiedEventDispatcher(executorService, listenerRegistry, List.of(transport1));
        Object listener = new Object();
        when(listenerRegistry.isRegistered(listener)).thenReturn(true);

        boolean result = dispatcher.isRegistered(listener);

        assertTrue(result);
    }

    static class TestEvent implements Event {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }

    static class TestEventListener implements EventListener {
        private final AtomicBoolean called = new AtomicBoolean(false);

        @Override
        public List<Class<? extends Event>> events() {
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
