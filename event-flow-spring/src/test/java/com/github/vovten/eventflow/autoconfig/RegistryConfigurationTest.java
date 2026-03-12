package com.github.vovten.eventflow.autoconfig;

import com.github.vovten.eventflow.autoconfig.config.RegistryConfiguration;
import com.github.vovten.eventflow.registry.EventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringInterfaceEventListenerRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RegistryConfiguration}.
 * <p>
 * Note: Full integration tests for registry configuration are covered in
 * {@link com.github.vovten.eventflow.registry.EventListenerRegistryIntegrationTest}.
 * This test class focuses on configuration validation only.
 */
class RegistryConfigurationTest {

    @Test
    @DisplayName("RegistryConfiguration should be instantiable with properties")
    void registryConfigurationShouldBeInstantiableWithProperties() {
        // given
        EventFlowProperties properties = new EventFlowProperties();
        properties.setScanPackages("com.example.listener");

        // when
        RegistryConfiguration config = new RegistryConfiguration(properties);

        // then
        assertThat(config).isNotNull();
    }

    @Test
    @DisplayName("Should throw exception when scan-packages is empty in properties")
    void shouldThrowExceptionWhenScanPackagesIsEmptyInProperties() {
        // given
        EventFlowProperties properties = new EventFlowProperties();
        properties.setScanPackages("");
        RegistryConfiguration config = new RegistryConfiguration(properties);
        var context = new AnnotationConfigApplicationContext();
        context.refresh();

        // when & then
        assertThatThrownBy(() -> config.springAnnotationEventListenerRegistry(context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("event-flow.scan-packages must be configured");
    }

    @Test
    @DisplayName("Should create interface listener registry")
    void shouldCreateInterfaceListenerRegistry() {
        // given
        EventFlowProperties properties = new EventFlowProperties();
        properties.setScanPackages("com.example.listener");
        RegistryConfiguration config = new RegistryConfiguration(properties);
        var context = new AnnotationConfigApplicationContext();
        context.refresh();

        // when
        EventListenerRegistry registry = config.springInterfaceEventListenerRegistry(context);

        // then
        assertThat(registry).isNotNull();
        assertThat(registry.name()).containsIgnoringCase("interface");
        context.close();
    }

    @Test
    @DisplayName("Should create composite registry from multiple registries")
    void shouldCreateCompositeRegistryFromMultipleRegistries() {
        // given
        EventFlowProperties properties = new EventFlowProperties();
        properties.setScanPackages("com.example.listener");
        RegistryConfiguration config = new RegistryConfiguration(properties);

        var context1 = new AnnotationConfigApplicationContext();
        context1.refresh();
        var registry1 = new SpringInterfaceEventListenerRegistry(context1);

        var context2 = new AnnotationConfigApplicationContext();
        context2.refresh();
        var registry2 = new SpringInterfaceEventListenerRegistry(context2);

        // when
        EventListenerRegistry composite = config.eventListenerRegistry(List.of(registry1, registry2));

        // then
        assertThat(composite).isNotNull();
        assertThat(composite.name()).containsIgnoringCase("composite");

        context1.close();
        context2.close();
    }

    @Test
    @DisplayName("Should throw exception when creating composite with empty registries list")
    void shouldThrowExceptionWhenCreatingCompositeWithEmptyRegistriesList() {
        // given
        EventFlowProperties properties = new EventFlowProperties();
        properties.setScanPackages("com.example.listener");
        RegistryConfiguration config = new RegistryConfiguration(properties);

        // when & then
        assertThatThrownBy(() -> config.eventListenerRegistry(List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one registry must be provided");
    }

    @Test
    @DisplayName("Should create composite registry from single registry")
    void shouldCreateCompositeRegistryFromSingleRegistry() {
        // given
        EventFlowProperties properties = new EventFlowProperties();
        properties.setScanPackages("com.example.listener");
        RegistryConfiguration config = new RegistryConfiguration(properties);

        var context = new AnnotationConfigApplicationContext();
        context.refresh();
        var registry = new SpringInterfaceEventListenerRegistry(context);

        // when
        EventListenerRegistry composite = config.eventListenerRegistry(List.of(registry));

        // then
        assertThat(composite).isNotNull();
        assertThat(composite.name()).containsIgnoringCase("composite");
        context.close();
    }
}
