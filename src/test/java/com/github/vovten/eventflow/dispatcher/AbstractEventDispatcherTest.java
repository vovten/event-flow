package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.registry.EventListenerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AbstractEventDispatcher.
 */
class AbstractEventDispatcherTest {

    private ExecutorService executorService;
    private EventListenerRegistry listenerRegistry;
    private TestEventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        listenerRegistry = mock(EventListenerRegistry.class);
        dispatcher = new TestEventDispatcher(executorService, listenerRegistry);
    }

    @Test
    void testDispatch_NoListeners_NoAction() {
        when(listenerRegistry.getListeners(any())).thenReturn(List.of());

        TestEvent event = new TestEvent();
        dispatcher.dispatch(event);

        verify(listenerRegistry).getListeners(event);
    }

    @Test
    void testDispatch_WithListeners_ExecutesAsync() throws InterruptedException {
        TestEventListener listener = new TestEventListener();
        when(listenerRegistry.getListeners(any())).thenReturn(List.of(listener));

        TestEvent event = new TestEvent();
        dispatcher.dispatch(event);

        Thread.sleep(100);
        assertTrue(listener.wasCalled());
    }

    @Test
    void testDispatch_MultipleListeners_AllExecuted() throws InterruptedException {
        TestEventListener listener1 = new TestEventListener();
        TestEventListener listener2 = new TestEventListener();
        when(listenerRegistry.getListeners(any())).thenReturn(List.of(listener1, listener2));

        TestEvent event = new TestEvent();
        dispatcher.dispatch(event);

        Thread.sleep(100);
        assertTrue(listener1.wasCalled());
        assertTrue(listener2.wasCalled());
    }

    @Test
    void testRegister_DelegatesToRegistry() {
        Object listener = new Object();
        when(listenerRegistry.isRegistered(listener)).thenReturn(false);

        dispatcher.register(listener);

        verify(listenerRegistry).register(listener);
    }

    @Test
    void testRegister_AlreadyRegistered_DoesNotRegisterAgain() {
        Object listener = new Object();
        when(listenerRegistry.isRegistered(listener)).thenReturn(true);

        dispatcher.register(listener);

        verify(listenerRegistry, never()).register(any());
    }

    @Test
    void testIsRegistered_DelegatesToRegistry() {
        Object listener = new Object();
        when(listenerRegistry.isRegistered(listener)).thenReturn(true);

        boolean result = dispatcher.isRegistered(listener);

        assertTrue(result);
        verify(listenerRegistry).isRegistered(listener);
    }

    @Test
    void testIsRegistered_NotRegistered() {
        Object listener = new Object();
        when(listenerRegistry.isRegistered(listener)).thenReturn(false);

        boolean result = dispatcher.isRegistered(listener);

        assertFalse(result);
    }

    @Test
    void testDispatch_ListenerThrowsException_DoesNotAffectOtherListeners() throws InterruptedException {
        FailingEventListener failingListener = new FailingEventListener();
        TestEventListener successListener = new TestEventListener();
        when(listenerRegistry.getListeners(any())).thenReturn(List.of(failingListener, successListener));

        TestEvent event = new TestEvent();
        dispatcher.dispatch(event);

        Thread.sleep(100);
        assertTrue(failingListener.wasCalled());
        assertTrue(successListener.wasCalled());
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

    static class FailingEventListener implements EventListener {
        private final AtomicBoolean called = new AtomicBoolean(false);

        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
            called.set(true);
            throw new RuntimeException("Listener error");
        }

        boolean wasCalled() {
            return called.get();
        }
    }

    static class TestEventDispatcher extends AbstractEventDispatcher {
        protected TestEventDispatcher(ExecutorService executorService, EventListenerRegistry listenerRegistry) {
            super(executorService, listenerRegistry);
        }
    }
}
