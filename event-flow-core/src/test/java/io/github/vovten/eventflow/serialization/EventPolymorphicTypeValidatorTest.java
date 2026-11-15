package io.github.vovten.eventflow.serialization;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link EventPolymorphicTypeValidator} class lookup.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("EventPolymorphicTypeValidator Class Lookup Tests")
class EventPolymorphicTypeValidatorTest {

    @Test
    @DisplayName("DEFECT-4.4: Falls back to the validator loader when the TCCL cannot resolve a class")
    void shouldFallBackToOwnClassLoader() {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(new ClassLoader() {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                throw new ClassNotFoundException(name);
            }
        });
        try {
            EventPolymorphicTypeValidator validator = new EventPolymorphicTypeValidator();

            assertThat(validator.classExists(EventPolymorphicTypeValidator.class.getName()))
                    .as("A missing TCCL must not deny a legitimate class")
                    .isTrue();
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    @DisplayName("DEFECT-4.4: Caches negative class lookups")
    void shouldCacheNegativeLookups() {
        AtomicInteger loads = new AtomicInteger();
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(new ClassLoader() {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                loads.incrementAndGet();
                throw new ClassNotFoundException(name);
            }
        });
        try {
            EventPolymorphicTypeValidator validator = new EventPolymorphicTypeValidator();

            assertThat(validator.classExists("com.example.Missing")).isFalse();
            int afterFirstLookup = loads.get();
            assertThat(validator.classExists("com.example.Missing")).isFalse();
            assertThat(loads)
                    .as("A negative lookup must be cached instead of repeating Class.forName")
                    .hasValue(afterFirstLookup);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }
}
