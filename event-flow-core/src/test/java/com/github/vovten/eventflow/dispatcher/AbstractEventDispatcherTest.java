package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.registry.EventListenerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
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
@DisplayName("AbstractEventDispatcher Tests")
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
    @DisplayName("Should do nothing when no listeners")
    void shouldDoNothingWhenNoListeners() {
        when(listenerRegistry.getListeners(any())).thenReturn(List.of());

        TestEvent event = new TestEvent();
        dispatcher.dispatch(event);

        verify(listenerRegistry).getListeners(event);
    }

    @Test
    @DisplayName("Should execute listener asynchronously")
    void shouldExecuteListenerAsynchronously() throws InterruptedException {
        TestEventListener listener = new TestEventListener();
        when(listenerRegistry.getListeners(any())).thenReturn(List.of(listener));

        TestEvent event = new TestEvent();
        dispatcher.dispatch(event);

        Thread.sleep(100);
        assertTrue(listener.wasCalled());
    }

    @Test
    @DisplayName("Should execute all listeners")
    void shouldExecuteAllListeners() throws InterruptedException {
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
    @DisplayName("Should delegate register to registry")
    void shouldDelegateRegisterToRegistry() {
        Object listener = new Object();
        when(listenerRegistry.isRegistered(listener)).thenReturn(false);

        dispatcher.register(listener);

        verify(listenerRegistry).register(listener);
    }

    @Test
    @DisplayName("Should not register already registered listener")
    void shouldNotRegisterAlreadyRegisteredListener() {
        Object listener = new Object();
        when(listenerRegistry.isRegistered(listener)).thenReturn(true);

        dispatcher.register(listener);

        verify(listenerRegistry, never()).register(any());
    }

    @Test
    @DisplayName("Should delegate isRegistered to registry")
    void shouldDelegateIsRegisteredToRegistry() {
        Object listener = new Object();
        when(listenerRegistry.isRegistered(listener)).thenReturn(true);

        boolean result = dispatcher.isRegistered(listener);

        assertTrue(result);
        verify(listenerRegistry).isRegistered(listener);
    }

    @Test
    @DisplayName("Should return false for unregistered listener")
    void shouldReturnFalseForUnregisteredListener() {
        Object listener = new Object();
        when(listenerRegistry.isRegistered(listener)).thenReturn(false);

        boolean result = dispatcher.isRegistered(listener);

        assertFalse(result);
    }

    @Test
    @DisplayName("Should not affect other listeners when one throws exception")
    void shouldNotAffectOtherListenersWhenOneThrowsException() throws InterruptedException {
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
