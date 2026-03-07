package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for EventListenerRegistryBuilder.
 */
@DisplayName("EventListenerRegistryBuilder Tests")
class EventListenerRegistryBuilderTest {

    @Test
    @DisplayName("Should throw exception when building without listeners")
    void shouldThrowExceptionWhenBuildingWithoutListeners() {
        assertThrows(IllegalStateException.class, () ->
                EventListenerRegistryBuilder.create()
                        .build());
    }

    @Test
    @DisplayName("Should build annotation-based registry without Spring")
    void shouldBuildAnnotationBasedRegistryWithoutSpring() {
        EventListenerRegistry registry = EventListenerRegistryBuilder.create()
                .withAnnotationListeners()
                .build();

        assertNotNull(registry);
        assertTrue(registry instanceof AnnotationEventListenerRegistry);
    }

    @Test
    @DisplayName("Should build interface-based registry without Spring")
    void shouldBuildInterfaceBasedRegistryWithoutSpring() {
        EventListenerRegistry registry = EventListenerRegistryBuilder.create()
                .withInterfaceListeners()
                .build();

        assertNotNull(registry);
        assertTrue(registry instanceof InterfaceEventListenerRegistry);
    }

    @Test
    @DisplayName("Should build composite registry with multiple listener types")
    void shouldBuildCompositeRegistryWithMultipleListenerTypes() {
        EventListenerRegistry registry = EventListenerRegistryBuilder.create()
                .withAnnotationListeners()
                .withInterfaceListeners()
                .build();

        assertNotNull(registry);
        assertTrue(registry instanceof CompositeEventListenerRegistry);
    }

    @Test
    @DisplayName("Should throw exception for null Spring context")
    void shouldThrowExceptionForNullSpringContext() {
        assertThrows(IllegalArgumentException.class, () ->
                EventListenerRegistryBuilder.create()
                        .withSpring(null, "com.example")
                        .build());
    }

    @Test
    @DisplayName("Should throw exception for empty scan package")
    void shouldThrowExceptionForEmptyScanPackage() {
        ApplicationContext context = mock(ApplicationContext.class);
        assertThrows(IllegalArgumentException.class, () ->
                EventListenerRegistryBuilder.create()
                        .withSpring(context, "")
                        .build());
    }

    @Test
    @DisplayName("Should throw exception for null scan package")
    void shouldThrowExceptionForNullScanPackage() {
        ApplicationContext context = mock(ApplicationContext.class);
        assertThrows(IllegalArgumentException.class, () ->
                EventListenerRegistryBuilder.create()
                        .withSpring(context, null)
                        .build());
    }

    @Test
    @DisplayName("Should throw exception for invalid scan package")
    void shouldThrowExceptionForInvalidScanPackage() {
        ApplicationContext context = mock(ApplicationContext.class);
        // Note: Invalid package name throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () ->
                EventListenerRegistryBuilder.create()
                        .withSpring(context, "invalid-package")
                        .withAnnotationListeners()
                        .build());
    }

    @Test
    @DisplayName("Should add custom registry")
    void shouldAddCustomRegistry() {
        AnnotationEventListenerRegistry customRegistry = new AnnotationEventListenerRegistry();

        EventListenerRegistry registry = EventListenerRegistryBuilder.create()
                .withCustomRegistry(customRegistry)
                .build();

        assertNotNull(registry);
        assertEquals(customRegistry, registry);
    }

    @Test
    @DisplayName("Should ignore null custom registry")
    void shouldIgnoreNullCustomRegistry() {
        assertThrows(IllegalStateException.class, () ->
                EventListenerRegistryBuilder.create()
                        .withCustomRegistry(null)
                        .build());
    }

    @Test
    @DisplayName("Should apply decorator to registry")
    void shouldApplyDecoratorToRegistry() {
        EventListenerRegistry registry = EventListenerRegistryBuilder.create()
                .withAnnotationListeners()
                .withDecorator(r -> new CompositeEventListenerRegistry(List.of(r)))
                .build();

        assertNotNull(registry);
        assertTrue(registry instanceof CompositeEventListenerRegistry);
    }

    @Test
    @DisplayName("Should ignore null decorator")
    void shouldIgnoreNullDecorator() {
        EventListenerRegistry registry = EventListenerRegistryBuilder.create()
                .withAnnotationListeners()
                .withDecorator(null)
                .build();

        assertNotNull(registry);
        assertTrue(registry instanceof AnnotationEventListenerRegistry);
    }

    @Test
    @DisplayName("Should build and log configuration")
    void shouldBuildAndLogConfiguration() {
        EventListenerRegistry registry = EventListenerRegistryBuilder.create()
                .withAnnotationListeners()
                .withInterfaceListeners()
                .buildAndLog();

        assertNotNull(registry);
        assertTrue(registry instanceof CompositeEventListenerRegistry);
    }

    @Test
    @DisplayName("Should register annotation listener")
    void shouldRegisterAnnotationListener() {
        AnnotationEventListenerRegistry registry = (AnnotationEventListenerRegistry) EventListenerRegistryBuilder.create()
                .withAnnotationListeners()
                .build();

        TestAnnotatedListener listener = new TestAnnotatedListener();
        registry.register(listener);

        assertEquals(1, registry.listenerCount());
    }

    @Test
    @DisplayName("Should register interface listener")
    void shouldRegisterInterfaceListener() {
        InterfaceEventListenerRegistry registry = (InterfaceEventListenerRegistry) EventListenerRegistryBuilder.create()
                .withInterfaceListeners()
                .build();

        TestInterfaceListener listener = new TestInterfaceListener();
        registry.register(listener);

        assertEquals(1, registry.listenerCount());
    }

    static class TestAnnotatedListener {
        @com.github.vovten.eventflow.annotation.EventListener
        public void handleEvent(Event event) {
        }
    }

    static class TestInterfaceListener implements com.github.vovten.eventflow.EventListener {
        @Override
        public java.util.List<Class<? extends Event>> events() {
            return List.of(Event.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }
}
