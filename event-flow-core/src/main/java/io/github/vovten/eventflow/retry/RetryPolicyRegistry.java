package io.github.vovten.eventflow.retry;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of reusable named {@link RetryPolicy retry policies}.
 * <p>
 * Populated at startup from type-level annotations and configuration. Duplicate
 * names and blank names are configuration errors and fail fast.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class RetryPolicyRegistry {

    private final Map<String, ResolvedRetryPolicy> policies = new ConcurrentHashMap<>();

    /**
     * Registers a named policy.
     *
     * @param policy the resolved policy (must have a non-blank name)
     * @throws IllegalArgumentException if the name is blank
     * @throws IllegalStateException    if a policy with the same name is already registered
     */
    public void register(ResolvedRetryPolicy policy) {
        Objects.requireNonNull(policy, "policy must not be null");
        String name = policy.name();
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Named retry policy must have a non-blank name");
        }
        if (ResolvedRetryPolicy.DEFAULT_POLICY_NAME.equals(name)) {
            throw new IllegalArgumentException("Retry policy name 'default' is reserved");
        }
        ResolvedRetryPolicy previous = policies.putIfAbsent(name, policy);
        if (previous != null) {
            throw new IllegalStateException("Duplicate retry policy: " + name);
        }
    }

    /**
     * Finds a policy by name.
     *
     * @param name the policy name
     * @return the policy, or empty if not registered
     */
    public Optional<ResolvedRetryPolicy> find(String name) {
        if (name == null || name.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(policies.get(name));
    }

    /**
     * Resolves a policy by name.
     *
     * @param name the policy name
     * @return the registered policy
     * @throws IllegalArgumentException if the policy is not registered
     */
    public ResolvedRetryPolicy resolve(String name) {
        return find(name).orElseThrow(
                () -> new IllegalArgumentException("Unknown retry policy: " + name));
    }

    /**
     * Returns whether a policy with the given name is registered.
     *
     * @param name the policy name
     * @return {@code true} if registered
     */
    public boolean contains(String name) {
        return find(name).isPresent();
    }

    /**
     * Returns the number of registered policies.
     *
     * @return the registry size
     */
    public int size() {
        return policies.size();
    }
}
