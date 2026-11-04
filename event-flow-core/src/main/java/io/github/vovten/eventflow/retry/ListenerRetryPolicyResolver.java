package io.github.vovten.eventflow.retry;

import io.github.vovten.eventflow.EventListener;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves and registers named processing-retry policies for annotation-based
 * event listeners.
 * <p>
 * This class owns the retry-specific part of listener registration:
 * <ul>
 *   <li>scanning type-level and method-level {@link RetryPolicy} declarations;</li>
 *   <li>registering them in the shared {@link RetryPolicyRegistry};</li>
 *   <li>resolving a listener's declaration, named reference or the default policy.</li>
 * </ul>
 * {@code EventListenerRegistry} delegates to this class so that its own
 * responsibility stays limited to handler discovery and registration.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class ListenerRetryPolicyResolver {

    private final RetryPolicyRegistry retryPolicyRegistry;
    private final ResolvedRetryPolicy defaultRetryPolicy;
    private final Map<String, RetryPolicyOverride> overrides;
    private final Set<Class<?>> preparedPolicyTypes = ConcurrentHashMap.newKeySet();

    /**
     * Creates a resolver without configuration overrides.
     *
     * @param retryPolicyRegistry the shared named policy registry (must not be null)
     * @param defaultRetryPolicy  the default policy (must not be null)
     */
    public ListenerRetryPolicyResolver(RetryPolicyRegistry retryPolicyRegistry,
                                       ResolvedRetryPolicy defaultRetryPolicy) {
        this(retryPolicyRegistry, defaultRetryPolicy, Map.of());
    }

    /**
     * Creates a resolver with named-policy configuration overrides.
     *
     * @param retryPolicyRegistry the shared named policy registry (must not be null)
     * @param defaultRetryPolicy  the default policy (must not be null)
     * @param overrides           overrides by policy name (must not be null)
     */
    public ListenerRetryPolicyResolver(RetryPolicyRegistry retryPolicyRegistry,
                                       ResolvedRetryPolicy defaultRetryPolicy,
                                       Map<String, RetryPolicyOverride> overrides) {
        this.retryPolicyRegistry = Objects.requireNonNull(
                retryPolicyRegistry, "retryPolicyRegistry must not be null");
        this.defaultRetryPolicy = Objects.requireNonNull(
                defaultRetryPolicy, "defaultRetryPolicy must not be null");
        this.overrides = Map.copyOf(Objects.requireNonNull(overrides, "overrides must not be null"));
    }

    /**
     * Scans and registers all policy declarations of the given class.
     * <p>
     * A type-level {@code @RetryPolicy} is declaration-only. Method-level
     * declarations are registered here as well; they are bound when the
     * annotated listener is registered.
     *
     * @param beanClass the class to scan (may differ from the bean class for proxies)
     */
    public void prepare(Class<?> beanClass) {
        Objects.requireNonNull(beanClass, "beanClass must not be null");
        if (!preparedPolicyTypes.add(beanClass)) {
            return;
        }
        RetryPolicy typePolicy = beanClass.getAnnotation(RetryPolicy.class);
        if (typePolicy != null) {
            registerDeclaredPolicy(RetryPolicyResolver.resolveDeclared(typePolicy, defaultRetryPolicy), beanClass);
        }
        for (Method method : beanClass.getMethods()) {
            if (method.isAnnotationPresent(EventListener.class)
                    && method.isAnnotationPresent(RetryPolicy.class)) {
                RetryPolicy methodPolicy = method.getAnnotation(RetryPolicy.class);
                registerDeclaredPolicy(
                        RetryPolicyResolver.resolveDeclared(methodPolicy, defaultRetryPolicy), method);
            }
        }
    }

    /**
     * Registers a dedicated named-policy holder class.
     *
     * @param policyType the class to scan
     */
    public void registerPolicyType(Class<?> policyType) {
        prepare(policyType);
    }

    /**
     * Resolves the effective policy for a listener method.
     * <p>
     * A method-level declaration declares and binds its own policy. Otherwise a
     * non-blank {@code @EventListener.retryPolicy} reference is resolved from the
     * registry. Otherwise the default policy applies.
     *
     * @param listenerMethod the listener method
     * @return the effective policy, never {@code null}
     * @throws IllegalArgumentException if a declaration and a reference are both present,
     *                                  or the referenced policy is unknown
     * @throws IllegalStateException    if two different policies share the same name
     */
    public ResolvedRetryPolicy resolve(Method listenerMethod) {
        Objects.requireNonNull(listenerMethod, "listenerMethod must not be null");
        RetryPolicy declared = listenerMethod.getAnnotation(RetryPolicy.class);
        EventListener annotation = listenerMethod.getAnnotation(EventListener.class);
        String reference = annotation != null ? annotation.retryPolicy() : "";
        boolean hasReference = reference != null && !reference.isBlank();
        if (declared != null && hasReference) {
            throw new IllegalArgumentException(
                    "Listener " + listenerMethod.getDeclaringClass().getName() + "#" + listenerMethod.getName()
                            + " has both @RetryPolicy and @EventListener(retryPolicy=...); choose one");
        }
        if (declared != null) {
            ResolvedRetryPolicy resolved = RetryPolicyResolver.resolveDeclared(declared, defaultRetryPolicy);
            registerDeclaredPolicy(resolved, listenerMethod);
            return retryPolicyRegistry.resolve(resolved.name());
        }
        if (hasReference) {
            return retryPolicyRegistry.resolve(reference);
        }
        return defaultRetryPolicy;
    }

    /**
     * Resolves a named policy reference.
     * <p>
     * A blank name resolves to the default policy. An unknown name fails fast.
     *
     * @param name the named policy reference, may be blank
     * @return the resolved policy, never {@code null}
     * @throws IllegalArgumentException if the referenced policy is unknown
     */
    public ResolvedRetryPolicy resolveReference(String name) {
        if (name == null || name.isBlank()) {
            return defaultRetryPolicy;
        }
        return retryPolicyRegistry.resolve(name);
    }

    /**
     * Returns the default policy used when a listener has no declaration or reference.
     *
     * @return the default policy
     */
    public ResolvedRetryPolicy defaultPolicy() {
        return defaultRetryPolicy;
    }

    private void registerDeclaredPolicy(ResolvedRetryPolicy policy, Object source) {
        ResolvedRetryPolicy effective = applyOverride(policy);
        Optional<ResolvedRetryPolicy> existing = retryPolicyRegistry.find(effective.name());
        if (existing.isPresent()) {
            if (!existing.get().equals(effective)) {
                throw new IllegalStateException(
                        "Duplicate retry policy '" + effective.name() + "' declared at " + source);
            }
            return;
        }
        retryPolicyRegistry.register(effective);
    }

    private ResolvedRetryPolicy applyOverride(ResolvedRetryPolicy policy) {
        RetryPolicyOverride override = overrides.get(policy.name());
        return override != null ? override.applyTo(policy) : policy;
    }
}
