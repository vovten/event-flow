package com.github.vovten.eventflow.collection;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for SpringInterfaceBasedEventListenerRegistry
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
class SpringInterfaceBasedEventListenerRegistryTest {

    private SpringInterfaceBasedEventListenerRegistry registry;
    private ExecutorService executorService;

    @BeforeEach
    void setUp() {
        executorService = Executors.newSingleThreadExecutor();
    }

    @Test
    void testConstructorWithExecutorService() {
        registry = new SpringInterfaceBasedEventListenerRegistry(executorService);
        assertNotNull(registry);
        assertFalse(registry.hasListeners());
    }

    @Test
    void testRegisterEventListener() {
        registry = new SpringInterfaceBasedEventListenerRegistry(executorService);
        EventListener listener = new TestEventListener();
        registry.register(listener);
        assertTrue(registry.hasListeners());
    }

    @Test
    void testDispatchEvent() throws InterruptedException {
        registry = new SpringInterfaceBasedEventListenerRegistry(executorService);
        TestEventListener listener = new TestEventListener();
        registry.register(listener);
        TestEvent event = new TestEvent();
        assertTrue(registry.dispatch(event));
        Thread.sleep(100);
        assertTrue(listener.eventReceived);
    }

    @Test
    void testIsRegistered() {
        registry = new SpringInterfaceBasedEventListenerRegistry(executorService);
        EventListener listener = new TestEventListener();
        assertFalse(registry.isRegistered(listener));
        registry.register(listener);
        assertTrue(registry.isRegistered(listener));
    }

    @Test
    void testInitWithApplicationContext() {
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        TestEventListener listener = new TestEventListener();
        when(applicationContext.getBeansOfType(EventListener.class))
                .thenReturn(java.util.Collections.singletonMap("testListener", listener));
        registry = new SpringInterfaceBasedEventListenerRegistry(executorService, applicationContext);
        assertTrue(registry.hasListeners());
    }

    @Test
    void testMergeUnsupported() {
        registry = new SpringInterfaceBasedEventListenerRegistry(executorService);
        EventListenerRegistry otherRegistry = mock(EventListenerRegistry.class);
        assertThrows(UnsupportedOperationException.class, () -> registry.merge(otherRegistry));
    }

    @Test
    void testListenerCount() {
        registry = new SpringInterfaceBasedEventListenerRegistry(executorService);
        assertEquals(0, registry.listenerCount());
        registry.register(new TestEventListener());
        assertEquals(1, registry.listenerCount());
    }

    @Test
    void testDispatchWithNoListeners() {
        registry = new SpringInterfaceBasedEventListenerRegistry(executorService);
        assertFalse(registry.dispatch(new TestEvent()));
    }

    @Test
    void testDispatchWithGenericEvent() throws InterruptedException {
        registry = new SpringInterfaceBasedEventListenerRegistry(executorService);
        GenericEventListener listener = new GenericEventListener();
        registry.register(listener);
        TestEvent event = new TestEvent();
        assertTrue(registry.dispatch(event));
        Thread.sleep(100);
        assertTrue(listener.eventReceived);
    }

    @Test
    void testIsRegisteredWithNonEventListener() {
        registry = new SpringInterfaceBasedEventListenerRegistry(executorService);
        Object nonListener = new Object();
        assertFalse(registry.isRegistered(nonListener));
    }

    static class TestEvent implements Event {
    }

    static class TestEventListener implements EventListener {
        boolean eventReceived = false;

        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
            eventReceived = true;
        }
    }

    static class GenericEventListener implements EventListener {
        boolean eventReceived = false;

        @Override
        public List<Class<? extends Event>> events() {
            return List.of(Event.class);
        }

        @Override
        public void onEvent(Event event) {
            eventReceived = true;
        }
    }
}
