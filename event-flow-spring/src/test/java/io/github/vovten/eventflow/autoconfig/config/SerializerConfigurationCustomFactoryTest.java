package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for user-provided {@link EventSerializerFactory} beans.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("SerializerConfiguration Custom Factory Tests")
class SerializerConfigurationCustomFactoryTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class, SerializerConfiguration.class);

    @Test
    @DisplayName("DEFECT-7.5: A user-defined EventSerializerFactory replaces the auto-configured one")
    void shouldBackOffForUserFactory() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(EventSerializerFactory.class);
            assertThat(context.getBean(EventSerializerFactory.class))
                    .isSameAs(TestConfig.USER_FACTORY);
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class TestConfig {

        static final EventSerializerFactory USER_FACTORY = new EventSerializerFactory();

        @Bean
        EventFlowProperties eventFlowProperties() {
            return new EventFlowProperties();
        }

        @Bean
        EventSerializerFactory customSerializerFactory() {
            return USER_FACTORY;
        }
    }
}
