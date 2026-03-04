package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for SpringInterfaceBasedEventListenerRegistry
 */
class SpringInterfaceBasedEventListenerRegistryTest {

    @Test
    void testConstructorWithApplicationContext() {
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        SpringInterfaceBasedEventListenerRegistry registry = new SpringInterfaceBasedEventListenerRegistry(applicationContext);
        assertNotNull(registry);
        assertTrue(registry.isEmpty());
    }

    @Test
    void testRegisterEventListener() {
        SpringInterfaceBasedEventListenerRegistry registry = new SpringInterfaceBasedEventListenerRegistry();
        TestEventListener listener = new TestEventListener();
        registry.register(listener);
        assertFalse(registry.isEmpty());
    }

    @Test
    void testGetListeners() {
        SpringInterfaceBasedEventListenerRegistry registry = new SpringInterfaceBasedEventListenerRegistry();
        TestEventListener listener = new TestEventListener();
        registry.register(listener);
        TestEvent event = new TestEvent();
        List<EventListener> listeners = registry.getListeners(event);
        assertEquals(1, listeners.size());
    }

    @Test
    void testIsRegistered() {
        SpringInterfaceBasedEventListenerRegistry registry = new SpringInterfaceBasedEventListenerRegistry();
        TestEventListener listener = new TestEventListener();
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
        SpringInterfaceBasedEventListenerRegistry registry = new SpringInterfaceBasedEventListenerRegistry(applicationContext);
        assertFalse(registry.isEmpty());
    }

    @Test
    void testMergeUnsupported() {
        SpringInterfaceBasedEventListenerRegistry registry = new SpringInterfaceBasedEventListenerRegistry();
        EventListenerRegistry otherRegistry = mock(EventListenerRegistry.class);
        assertThrows(UnsupportedOperationException.class, () -> registry.merge(otherRegistry));
    }

    @Test
    void testListenerCount() {
        SpringInterfaceBasedEventListenerRegistry registry = new SpringInterfaceBasedEventListenerRegistry();
        assertEquals(0, registry.listenerCount());
        registry.register(new TestEventListener());
        assertEquals(1, registry.listenerCount());
    }

    static class TestEvent implements Event {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
        
        public static TestEvent create() {
            return new TestEvent();
        }
    }

    static class TestEventListener implements EventListener {
        @Override
        public List<Class<? extends Event>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }
}
