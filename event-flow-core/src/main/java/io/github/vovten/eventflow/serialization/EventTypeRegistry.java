package io.github.vovten.eventflow.serialization;

import io.github.vovten.eventflow.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for controlling which event classes are allowed for deserialization.
 * <p>
 * This registry provides a security mechanism to prevent deserialization attacks
 * by maintaining a whitelist of allowed event classes and packages. Only classes
 * explicitly registered or classes from allowed packages can be deserialized.
 * <p>
 * By default, all classes from {@code io.github.vovten.eventflow} package are allowed.
 * <p>
 * This class is thread-safe and designed for use in multi-threaded environments.
 *
 * @author Vladimir Aleshkov
 * @since 2026-04-05
 */
public final class EventTypeRegistry {

    private static final Logger log = LoggerFactory.getLogger(EventTypeRegistry.class);

    private static final Set<String> ALLOWED_PACKAGES = ConcurrentHashMap.newKeySet();
    private static final Set<Class<? extends Event>> ALLOWED_CLASSES = ConcurrentHashMap.newKeySet();
    private static final Set<String> ALLOWED_CLASS_NAMES = ConcurrentHashMap.newKeySet();

    static {
        // Allow all event-flow classes by default
        ALLOWED_PACKAGES.add("io.github.vovten.eventflow");
    }

    private EventTypeRegistry() {
        // Utility class
    }

    /**
     * Allow all event classes from the specified package.
     * <p>
     * <b>Example:</b>
     * <pre>{@code
     * EventTypeRegistry.allowPackage("com.example.events");
     * }</pre>
     *
     * @param packageName the package name to allow
     * @throws IllegalArgumentException if packageName is null or empty
     */
    public static void allowPackage(String packageName) {
        if (packageName == null || packageName.isEmpty()) {
            throw new IllegalArgumentException("Package name must not be null or empty");
        }
        ALLOWED_PACKAGES.add(packageName);
        log.debug("Allowed package: {}", packageName);
    }

    /**
     * Allow a specific event class.
     * <p>
     * <b>Example:</b>
     * <pre>{@code
     * EventTypeRegistry.allowClass(OrderCreatedEvent.class);
     * }</pre>
     *
     * @param eventClass the event class to allow
     * @throws IllegalArgumentException if eventClass is null
     */
    public static void allowClass(Class<? extends Event> eventClass) {
        if (eventClass == null) {
            throw new IllegalArgumentException("Event class must not be null");
        }
        ALLOWED_CLASSES.add(eventClass);
        ALLOWED_CLASS_NAMES.add(eventClass.getName());
        log.debug("Allowed class: {}", eventClass.getName());
    }

    /**
     * Check if a class name is allowed by the registry.
     * <p>
     * A class is allowed if:
     * <ul>
     *   <li>It is explicitly in the allowed classes set, OR</li>
     *   <li>It belongs to one of the allowed packages</li>
     * </ul>
     *
     * @param className the fully-qualified class name to check
     * @return true if the class is allowed
     */
    public static boolean isAllowed(String className) {
        if (className == null || className.isEmpty()) {
            return false;
        }
        if (ALLOWED_CLASS_NAMES.contains(className)) {
            return true;
        }
        for (String allowedPackage : ALLOWED_PACKAGES) {
            if (className.startsWith(allowedPackage + ".") || className.equals(allowedPackage)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get all allowed package names.
     *
     * @return unmodifiable set of allowed package names
     */
    public static Set<String> getAllowedPackages() {
        return Collections.unmodifiableSet(ALLOWED_PACKAGES);
    }

    /**
     * Get all allowed event classes.
     *
     * @return unmodifiable set of allowed event classes
     */
    public static Set<Class<? extends Event>> getAllowedClasses() {
        return Collections.unmodifiableSet(ALLOWED_CLASSES);
    }

    /**
     * Clear all allowed packages and classes, then reset to defaults.
     * <p>
     * Package-private method intended for testing purposes only.
     * Use with caution as it affects global state.
     */
    static void clear() {
        ALLOWED_PACKAGES.clear();
        ALLOWED_CLASSES.clear();
        ALLOWED_CLASS_NAMES.clear();
        ALLOWED_PACKAGES.add("io.github.vovten.eventflow");
    }
}
