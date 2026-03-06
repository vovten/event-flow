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
 * Unit tests for SpringInterfaceEventListenerRegistry
 */
class SpringInterfaceEventListenerRegistryTest {

    private final ApplicationContext applicationContext = mock(ApplicationContext.class);

    @Test
    void testConstructorWithApplicationContext() {
        SpringInterfaceEventListenerRegistry registry = new SpringInterfaceEventListenerRegistry(applicationContext);
        assertNotNull(registry);
        assertEquals(0, registry.listenerCount());
    }

    @Test
    void testRegisterEventListener() {
        SpringInterfaceEventListenerRegistry registry = new SpringInterfaceEventListenerRegistry(applicationContext);
        TestEventListener listener = new TestEventListener();
        registry.register(listener);
        assertEquals(1, registry.listenerCount());
    }

    @Test
    void testGetListeners() {
        SpringInterfaceEventListenerRegistry registry = new SpringInterfaceEventListenerRegistry(applicationContext);
        TestEventListener listener = new TestEventListener();
        registry.register(listener);
        TestEvent event = new TestEvent();
        List<EventListener> listeners = registry.getListeners(event);
        assertEquals(1, listeners.size());
    }

    @Test
    void testIsRegistered() {
        SpringInterfaceEventListenerRegistry registry = new SpringInterfaceEventListenerRegistry(applicationContext);
        TestEventListener listener = new TestEventListener();
        assertFalse(registry.isRegistered(listener));
        registry.register(listener);
        assertTrue(registry.isRegistered(listener));
    }

    @Test
    void testInitWithApplicationContext() {
        TestEventListener listener = new TestEventListener();
        when(applicationContext.getBeansOfType(EventListener.class))
                .thenReturn(java.util.Collections.singletonMap("testListener", listener));
        SpringInterfaceEventListenerRegistry registry = new SpringInterfaceEventListenerRegistry(applicationContext);
        assertEquals(1, registry.listenerCount());
    }

    @Test
    void testUnregisterEventListener() {
        SpringInterfaceEventListenerRegistry registry = new SpringInterfaceEventListenerRegistry(applicationContext);
        TestEventListener listener = new TestEventListener();
        registry.register(listener);
        assertTrue(registry.isRegistered(listener));

        boolean result = registry.unregister(listener);
        assertTrue(result);
        assertFalse(registry.isRegistered(listener));
    }

    @Test
    void testUnregisterNonExistentListener() {
        SpringInterfaceEventListenerRegistry registry = new SpringInterfaceEventListenerRegistry(applicationContext);
        TestEventListener listener = new TestEventListener();
        boolean result = registry.unregister(listener);
        assertFalse(result);
    }

    @Test
    void testMergeUnsupported() {
        SpringInterfaceEventListenerRegistry registry = new SpringInterfaceEventListenerRegistry(applicationContext);
        EventListenerRegistry otherRegistry = mock(EventListenerRegistry.class);
        assertThrows(UnsupportedOperationException.class, () -> registry.merge(otherRegistry));
    }

    @Test
    void testListenerCount() {
        SpringInterfaceEventListenerRegistry registry = new SpringInterfaceEventListenerRegistry(applicationContext);
        assertEquals(0, registry.listenerCount());
        registry.register(new TestEventListener());
        assertEquals(1, registry.listenerCount());
    }

    @Test
    void testConstructorWithNullApplicationContext() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new SpringInterfaceEventListenerRegistry(null)
        );
        assertEquals("ApplicationContext is required", exception.getMessage());
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
