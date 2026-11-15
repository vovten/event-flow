package io.github.vovten.eventflow.serialization;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Polymorphic type validator for secure event deserialization.
 * <p>
 * This validator ensures that the class specified in the {@code @class} field during
 * deserialization is in the whitelist of allowed packages or classes. This prevents
 * deserialization attacks from malicious classes.
 * <p>
 * Validation uses {@link EventTypeRegistry} to verify class authorization.
 * <p>
 * Important: Jackson calls {@link #validateSubType} for the actual class from {@code @class},
 * not {@link #validateBaseType} which is called for the target type (e.g., Event.class).
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class EventPolymorphicTypeValidator extends PolymorphicTypeValidator.Base {

    private static final Logger log = LoggerFactory.getLogger(EventPolymorphicTypeValidator.class);

    /**
     * Bounded presence cache: both positive and negative lookups are cached so a
     * flood of unknown/denied classes cannot turn every deserialization attempt
     * into repeated {@code Class.forName} calls.
     */
    private final Cache<String, Boolean> classPresence = Caffeine.newBuilder()
            .maximumSize(10_000)
            .build();

    /**
     * Validate the base type (the type passed to readValue).
     * We allow this through as the actual validation happens in validateSubType.
     *
     * @param config the mapper configuration
     * @param baseType the Java type to validate
     * @return INDETERMINATE to allow subtype validation to proceed
     */
    @Override
    public Validity validateBaseType(MapperConfig<?> config, JavaType baseType) {
        // Base type (e.g., Event.class) is always allowed
        // Actual security validation happens in validateSubType
        return Validity.INDETERMINATE;
    }

    /**
     * Validate the subtype (the actual class from @class field).
     * This is where the security validation actually happens.
     *
     * @param config the mapper configuration
     * @param baseType the declared base type
     * @param subType the actual subtype from @class
     * @return ALLOWED if authorized, DENIED otherwise
     */
    @Override
    public Validity validateSubType(MapperConfig<?> config, JavaType baseType, JavaType subType) {
        Class<?> rawClass = subType.getRawClass();
        String className = rawClass.getName();

        if (!classExists(className)) {
            log.warn("Skipping event: class '{}' not found in classpath", className);
            return Validity.DENIED;
        }
        if (EventTypeRegistry.isAllowed(className)) {
            log.debug("Allowed event subtype: {}", className);
            return Validity.ALLOWED;
        }
        String packageName = getPackageName(className);
        log.error(
                "Blocked deserialization of unauthorized event class: '{}'.\n"
                + "To fix this, allow the class or its package using one of the methods below:\n"
                + "\n"
                + "  For Spring Boot applications, add to application.yml:\n"
                + "    event-flow:\n"
                + "      dispatcher:\n"
                + "        deserialization:\n"
                + "          allowed-event-packages:\n"
                + "            - {}\n"
                + "\n"
                + "  Or programmatically:\n"
                + "    EventTypeRegistry.allowPackage(\"{}\");\n"
                + "    EventTypeRegistry.allowClass({}.class);\n"
                + "\n"
                + "Currently allowed packages: {}",
                className,
                packageName,
                packageName,
                className,
                EventTypeRegistry.getAllowedPackages()
        );
        return Validity.DENIED;
    }

    /**
     * Check if class exists in classpath.
     * Uses caching for performance optimization.
     *
     * @param className fully qualified class name
     * @return true if class exists, false otherwise
     */
    boolean classExists(String className) {
        Boolean known = classPresence.getIfPresent(className);
        if (known != null) {
            return known;
        }
        boolean exists = resolveClass(className) != null;
        classPresence.put(className, exists);
        return exists;
    }

    /**
     * Resolves a class using the context class loader first and the validator's
     * own class loader as a fallback. Executor/producer threads may not have a
     * useful TCCL; falling back avoids a false "class not found" that would
     * silently deny a legitimate event.
     *
     * @param className fully qualified class name
     * @return the resolved class, or null when no loader can see it
     */
    private static Class<?> resolveClass(String className) {
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        if (tccl != null) {
            try {
                return Class.forName(className, false, tccl);
            } catch (ClassNotFoundException e) {
                log.debug("Class '{}' not visible to the context class loader, trying the library loader",
                        className);
            }
        }
        ClassLoader own = EventPolymorphicTypeValidator.class.getClassLoader();
        try {
            return Class.forName(className, false, own);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    /**
     * Extract package name from fully qualified class name.
     *
     * @param className fully qualified class name
     * @return package name
     */
    private String getPackageName(String className) {
        int lastDot = className.lastIndexOf('.');
        return lastDot > 0 ? className.substring(0, lastDot) : "";
    }
}
