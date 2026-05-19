package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.EventSubscriber;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SpringEventHandlerRegistryBuilder.
 * @since 1.0.0
 */
@DisplayName("SpringEventHandlerRegistryBuilder Tests")
class SpringEventHandlerRegistryBuilderTest {

    private GenericApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new GenericApplicationContext();
        applicationContext.refresh();
    }

    @Test
    @DisplayName("Should throw exception when building without handlers")
    void shouldThrowExceptionWhenBuildingWithoutHandlers() {
        assertThrows(IllegalStateException.class, () ->
                SpringEventHandlerRegistryBuilder.create(applicationContext)
                        .build());
    }

    @Test
    @DisplayName("Should throw exception for null context")
    void shouldThrowExceptionForNullContext() {
        assertThrows(IllegalArgumentException.class, () ->
                SpringEventHandlerRegistryBuilder.create(null));
    }

    @Test
    @DisplayName("Should build annotation-based registry with Spring")
    void shouldBuildAnnotationBasedRegistryWithSpring() {
        EventHandlerRegistry registry = SpringEventHandlerRegistryBuilder.create(applicationContext)
                .withAnnotationListeners("com.example")
                .build();

        assertNotNull(registry);
        assertInstanceOf(SpringEventListenerRegistry.class, registry);
    }

    @Test
    @DisplayName("Should throw exception for null scan package")
    void shouldThrowExceptionForNullScanPackage() {
        assertThrows(IllegalArgumentException.class, () ->
                SpringEventHandlerRegistryBuilder.create(applicationContext)
                        .withAnnotationListeners(null));
    }

    @Test
    @DisplayName("Should throw exception for empty scan package")
    void shouldThrowExceptionForEmptyScanPackage() {
        assertThrows(IllegalArgumentException.class, () ->
                SpringEventHandlerRegistryBuilder.create(applicationContext)
                        .withAnnotationListeners(""));
    }

    @Test
    @DisplayName("Should build interface-based registry with Spring")
    void shouldBuildInterfaceBasedRegistryWithSpring() {
        EventHandlerRegistry registry = SpringEventHandlerRegistryBuilder.create(applicationContext)
                .withInterfaceListeners()
                .build();

        assertNotNull(registry);
        assertInstanceOf(SpringEventSubscriberRegistry.class, registry);
    }

    @Test
    @DisplayName("Should build composite registry with multiple handler types")
    void shouldBuildCompositeRegistryWithMultipleHandlerTypes() {
        EventHandlerRegistry registry = SpringEventHandlerRegistryBuilder.create(applicationContext)
                .withAnnotationListeners("com.example")
                .withInterfaceListeners()
                .build();

        assertNotNull(registry);
        assertInstanceOf(CompositeEventHandlerRegistry.class, registry);
    }

    @Test
    @DisplayName("Should add custom registry")
    void shouldAddCustomRegistry() {
        SpringEventListenerRegistry customRegistry = new SpringEventListenerRegistry(applicationContext, "com.test");

        EventHandlerRegistry registry = SpringEventHandlerRegistryBuilder.create(applicationContext)
                .withCustomRegistry(customRegistry)
                .build();

        assertNotNull(registry);
        assertEquals(customRegistry, registry);
    }

    @Test
    @DisplayName("Should ignore null custom registry")
    void shouldIgnoreNullCustomRegistry() {
        assertThrows(IllegalStateException.class, () ->
                SpringEventHandlerRegistryBuilder.create(applicationContext)
                        .withCustomRegistry(null)
                        .build());
    }

    @Test
    @DisplayName("Should apply decorator to registry")
    void shouldApplyDecoratorToRegistry() {
        EventHandlerRegistry registry = SpringEventHandlerRegistryBuilder.create(applicationContext)
                .withAnnotationListeners("com.example")
                .withDecorator(r -> new CompositeEventHandlerRegistry(List.of(r)))
                .build();

        assertNotNull(registry);
        assertInstanceOf(CompositeEventHandlerRegistry.class, registry);
    }

    @Test
    @DisplayName("Should ignore null decorator")
    void shouldIgnoreNullDecorator() {
        EventHandlerRegistry registry = SpringEventHandlerRegistryBuilder.create(applicationContext)
                .withAnnotationListeners("com.example")
                .withDecorator(null)
                .build();

        assertNotNull(registry);
        assertInstanceOf(SpringEventListenerRegistry.class, registry);
    }

    @Test
    @DisplayName("Should build and log configuration")
    void shouldBuildAndLogConfiguration() {
        EventHandlerRegistry registry = SpringEventHandlerRegistryBuilder.create(applicationContext)
                .withAnnotationListeners("com.example")
                .withInterfaceListeners()
                .buildAndLog();

        assertNotNull(registry);
        assertInstanceOf(CompositeEventHandlerRegistry.class, registry);
    }

    @Test
    @DisplayName("Should combine custom registry with annotation listeners")
    void shouldCombineCustomRegistryWithAnnotationListeners() {
        SpringEventListenerRegistry customRegistry = new SpringEventListenerRegistry(applicationContext, "com.test");

        EventHandlerRegistry registry = SpringEventHandlerRegistryBuilder.create(applicationContext)
                .withAnnotationListeners("com.example")
                .withCustomRegistry(customRegistry)
                .build();

        assertNotNull(registry);
        assertInstanceOf(CompositeEventHandlerRegistry.class, registry);
    }

    @Test
    @DisplayName("Should combine custom registry with interface listeners")
    void shouldCombineCustomRegistryWithInterfaceListeners() {
        SpringEventSubscriberRegistry customRegistry = new SpringEventSubscriberRegistry(applicationContext);

        EventHandlerRegistry registry = SpringEventHandlerRegistryBuilder.create(applicationContext)
                .withInterfaceListeners()
                .withCustomRegistry(customRegistry)
                .build();

        assertNotNull(registry);
        assertInstanceOf(CompositeEventHandlerRegistry.class, registry);
    }

    @Test
    @DisplayName("Should apply multiple decorators")
    void shouldApplyMultipleDecorators() {
        EventHandlerRegistry registry = SpringEventHandlerRegistryBuilder.create(applicationContext)
                .withAnnotationListeners("com.example")
                .withDecorator(r -> new CompositeEventHandlerRegistry(List.of(r)))
                .withDecorator(r -> new CompositeEventHandlerRegistry(List.of(r)))
                .build();

        assertNotNull(registry);
        assertInstanceOf(CompositeEventHandlerRegistry.class, registry);
    }

    @Test
    @DisplayName("Should create full composite with all handler types")
    void shouldCreateFullCompositeWithAllHandlerTypes() {
        SpringEventListenerRegistry customRegistry = new SpringEventListenerRegistry(applicationContext, "com.test");

        EventHandlerRegistry registry = SpringEventHandlerRegistryBuilder.create(applicationContext)
                .withAnnotationListeners("com.example")
                .withInterfaceListeners()
                .withCustomRegistry(customRegistry)
                .withDecorator(r -> r)
                .build();

        assertNotNull(registry);
        assertInstanceOf(CompositeEventHandlerRegistry.class, registry);
    }

    static class TestEventSubscriber implements EventSubscriber {
        @Override
        public List<Class<?>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    static class TestEvent extends AbstractTraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }
}
