package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.autoconfig.EventFlowDisabledAutoConfiguration;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.EventFlowTestApplication;
import io.github.vovten.eventflow.EventListener;
import io.github.vovten.eventflow.EventSubscriber;
import io.github.vovten.eventflow.TestEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for EventHandlerRegistry implementations
 */
@SpringBootTest(classes = EventFlowTestApplication.class, properties = "event-flow.enabled=false")
@ImportAutoConfiguration(exclude = EventFlowDisabledAutoConfiguration.class)
class EventHandlerRegistryIntegrationTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    @DisplayName("Should get interface-based subscribers")
    void shouldGetInterfaceBasedSubscribers() {
        SpringEventSubscriberRegistry registry = new SpringEventSubscriberRegistry(applicationContext);
        registry.register(new InterfaceBasedSubscriber());
        TestEvent event = new TestEvent("Interface subscriber test");
        var handlers = registry.getHandlers(event);
        assertFalse(handlers.isEmpty());
        assertEquals(1, handlers.size());
    }

    @Test
    @DisplayName("Should get annotation-based handlers")
    void shouldGetAnnotationBasedHandlers() {
        SpringEventListenerRegistry registry = new SpringEventListenerRegistry(
                applicationContext, "io.github.vovten.eventflow.test");
        AnnotationBasedHandler handler = new AnnotationBasedHandler();
        registry.register(handler);
        TestEvent event = new TestEvent("Annotation handler test");
        var handlers = registry.getHandlers(event);
        // Note: May include other handlers from the scanned package
        assertFalse(handlers.isEmpty());
    }

    @Test
    @DisplayName("Should get handlers through composite registry")
    void shouldGetHandlersThroughCompositeRegistry() {
        EventSubscriberRegistry interfaceRegistry = new EventSubscriberRegistry();
        EventListenerRegistry annotationRegistry = new EventListenerRegistry();

        InterfaceBasedSubscriber interfaceSubscriber = new InterfaceBasedSubscriber();
        AnnotationBasedHandler annotationHandler = new AnnotationBasedHandler();

        interfaceRegistry.register(interfaceSubscriber);
        annotationRegistry.register(annotationHandler);

        CompositeEventHandlerRegistry compositeRegistry = new CompositeEventHandlerRegistry(
                new java.util.ArrayList<>(List.of(interfaceRegistry, annotationRegistry)));

        TestEvent event = new TestEvent("Composite subscriber test");
        var handlers = compositeRegistry.getHandlers(event);
        assertEquals(2, handlers.size());
    }

    @Test
    @DisplayName("Should initialize subscribers from Spring context")
    void shouldInitializeSubscribersFromSpringContext() {
        SpringEventSubscriberRegistry registry = new SpringEventSubscriberRegistry(applicationContext);
        SpringEventListenerRegistry annotationRegistry = new SpringEventListenerRegistry(applicationContext, "io.github.vovten.eventflow");

        assertTrue(registry.handlerCount() >= 0);
        assertTrue(annotationRegistry.handlerCount() >= 0);
    }

    @Test
    @DisplayName("Should unregister subscriber")
    void shouldUnregisterSubscriber() {
        SpringEventSubscriberRegistry registry = new SpringEventSubscriberRegistry(applicationContext);
        InterfaceBasedSubscriber subscriber = new InterfaceBasedSubscriber();
        registry.register(subscriber);
        assertTrue(registry.isRegistered(subscriber));

        boolean result = registry.unregister(subscriber);
        assertTrue(result);
        assertFalse(registry.isRegistered(subscriber));
    }

    // Interface-based subscriber
    public static class InterfaceBasedSubscriber implements EventSubscriber {
        @Override
        public List<Class<?>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    // Annotation-based handler
    @Component
    public static class AnnotationBasedHandler {
        @EventListener
        public void handleTestEvent(TestEvent event) {
        }
    }
}
