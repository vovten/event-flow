package com.github.vovten.eventflow.serialization;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.github.vovten.eventflow.event.Event;
import lombok.extern.slf4j.Slf4j;

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
 * @since 2026-04-05
 */
@Slf4j
public class EventPolymorphicTypeValidator extends PolymorphicTypeValidator.Base {

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

        // 1. Verify it's actually an Event (defense-in-depth)
        if (!Event.class.isAssignableFrom(rawClass)) {
            log.warn("Blocked deserialization of non-Event class: {}", className);
            return Validity.DENIED;
        }

        // 2. Check against the whitelist
        if (EventTypeRegistry.isAllowed(className)) {
            log.debug("Allowed event subtype: {}", className);
            return Validity.ALLOWED;
        }

        // 3. Block everything else
        log.warn("Blocked deserialization of unauthorized event class: {}", className);
        return Validity.DENIED;
    }
}
