package io.github.vovten.eventflow.retry;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Resolves {@link RetryPolicy} annotations and named policies into concrete
 * {@link ResolvedRetryPolicy} values.
 * <p>
 * Attributes left at their sentinel values inherit the supplied defaults. A
 * listener without a policy resolves to {@code null}: it has no local retries.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class RetryPolicyResolver {

    private RetryPolicyResolver() {
    }

    /**
     * Resolves a declared named policy.
     *
     * @param annotation the policy declaration, or {@code null} when the listener has no declaration
     * @param defaults   default policy used for unset attributes (must not be null)
     * @return the resolved policy, or {@code null} when the annotation is absent
     * @throws IllegalArgumentException if the declared name is blank
     */
    public static ResolvedRetryPolicy resolveDeclared(RetryPolicy annotation, ResolvedRetryPolicy defaults) {
        Objects.requireNonNull(defaults, "defaults must not be null");
        if (annotation == null) {
            return null;
        }
        if (annotation.name() == null || annotation.name().isBlank()) {
            throw new IllegalArgumentException("Retry policy name must not be blank");
        }
        int maxRetries = annotation.maxRetries() >= 0 ? annotation.maxRetries() : defaults.maxRetries();
        int localMaxRetries = annotation.localMaxRetries() >= 0
                ? annotation.localMaxRetries()
                : defaults.localMaxRetries();
        return new ResolvedRetryPolicy(
                annotation.name(),
                toTypeSet(annotation.retryOn()),
                toTypeSet(annotation.noRetryOn()),
                maxRetries,
                localMaxRetries,
                parseDuration(annotation.initialDelay(), defaults.initialDelay()),
                annotation.multiplier() >= 1.0 ? annotation.multiplier() : defaults.multiplier(),
                parseDuration(annotation.maxDelay(), defaults.maxDelay()),
                annotation.jitter() >= 0.0 ? annotation.jitter() : defaults.jitter());
    }

    /**
     * Resolves a named policy from the registry.
     *
     * @param name     the policy name, may be blank
     * @param registry the policy registry (must not be null)
     * @return the resolved policy, or {@code null} when the name is blank
     * @throws IllegalArgumentException if the named policy is unknown
     */
    public static ResolvedRetryPolicy resolveNamed(String name, RetryPolicyRegistry registry) {
        Objects.requireNonNull(registry, "registry must not be null");
        if (name == null || name.isBlank()) {
            return null;
        }
        return registry.resolve(name);
    }

    /**
     * Resolves the effective policy:
     * <ul>
     *   <li>a method-level declaration declares and binds its own named policy;</li>
     *   <li>otherwise a non-blank {@code namedPolicy} reference is resolved from the registry;</li>
     *   <li>otherwise the configured default policy applies.</li>
     * </ul>
     * Combining a method-level declaration with a {@code retryPolicy} reference is
     * ambiguous and fails fast.
     *
     * @param annotation    method-level declaration, may be {@code null}
     * @param namedPolicy   name referenced by {@code @EventListener.retryPolicy}, may be blank
     * @param registry      named policy registry (must not be null)
     * @param defaultPolicy configured default policy (must not be null)
     * @return the effective policy, never {@code null}
     * @throws IllegalArgumentException if both a declaration and a reference are present,
     *                                  or the referenced policy is unknown
     */
    public static ResolvedRetryPolicy resolveEffective(RetryPolicy annotation,
                                                       String namedPolicy,
                                                       RetryPolicyRegistry registry,
                                                       ResolvedRetryPolicy defaultPolicy) {
        Objects.requireNonNull(registry, "registry must not be null");
        Objects.requireNonNull(defaultPolicy, "defaultPolicy must not be null");
        boolean hasReference = namedPolicy != null && !namedPolicy.isBlank();
        if (annotation != null && hasReference) {
            throw new IllegalArgumentException(
                    "Both method-level @RetryPolicy and @EventListener(retryPolicy=...) are set");
        }
        if (annotation != null) {
            return resolveDeclared(annotation, defaultPolicy);
        }
        if (hasReference) {
            return registry.resolve(namedPolicy);
        }
        return defaultPolicy;
    }

    /**
     * Parses a duration such as {@code "100ms"}, {@code "5s"}, {@code "2m"},
     * {@code "1h"}, {@code "1d"}, {@code "250"} (milliseconds) or an ISO-8601
     * duration such as {@code "PT0.5S"}.
     *
     * @param value        the duration text, may be blank
     * @param defaultValue the value to use when {@code value} is blank
     * @return the parsed duration
     * @throws IllegalArgumentException if the text is not a valid non-negative duration
     */
    public static Duration parseDuration(String value, Duration defaultValue) {
        Objects.requireNonNull(defaultValue, "defaultValue must not be null");
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        String text = value.trim();
        String lower = text.toLowerCase(Locale.ROOT);
        if (lower.startsWith("p")) {
            try {
                return Duration.parse(text);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid duration: " + value, e);
            }
        }
        String number;
        long factor;
        if (lower.endsWith("ms")) {
            number = text.substring(0, text.length() - 2);
            factor = 1L;
        } else if (lower.endsWith("s")) {
            number = text.substring(0, text.length() - 1);
            factor = 1_000L;
        } else if (lower.endsWith("m")) {
            number = text.substring(0, text.length() - 1);
            factor = 60_000L;
        } else if (lower.endsWith("h")) {
            number = text.substring(0, text.length() - 1);
            factor = 3_600_000L;
        } else if (lower.endsWith("d")) {
            number = text.substring(0, text.length() - 1);
            factor = 86_400_000L;
        } else {
            number = text;
            factor = 1L;
        }
        try {
            double parsed = Double.parseDouble(number.trim());
            if (parsed < 0.0) {
                throw new IllegalArgumentException("Duration must not be negative: " + value);
            }
            return Duration.ofMillis((long) (parsed * factor));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid duration: " + value, e);
        }
    }

    private static Set<Class<? extends Throwable>> toTypeSet(Class<? extends Throwable>[] types) {
        return Arrays.stream(types).collect(Collectors.toUnmodifiableSet());
    }
}
