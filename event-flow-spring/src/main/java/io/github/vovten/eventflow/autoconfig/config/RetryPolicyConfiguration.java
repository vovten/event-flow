package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties.ProcessingRetryConfig;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties.RetryPolicyOverrideConfig;
import io.github.vovten.eventflow.retry.ListenerRetryPolicyResolver;
import io.github.vovten.eventflow.retry.ResolvedRetryPolicy;
import io.github.vovten.eventflow.retry.RetryDelayScheduler;
import io.github.vovten.eventflow.retry.RetryPolicyOverride;
import io.github.vovten.eventflow.retry.RetryPolicyRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ClassUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Auto-configuration for processing-retry policies.
 * <p>
 * Creates the shared {@link RetryPolicyRegistry}, the configured default policy,
 * the listener policy resolver and the non-blocking retry delay scheduler.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true")
public class RetryPolicyConfiguration {

    /**
     * Shared registry of named processing-retry policies.
     *
     * @return the empty policy registry
     */
    @Bean
    @ConditionalOnMissingBean
    public RetryPolicyRegistry retryPolicyRegistry() {
        return new RetryPolicyRegistry();
    }

    /**
     * Configured default processing-retry policy.
     *
     * @param properties event-flow properties
     * @return the default policy
     */
    @Bean("defaultRetryPolicy")
    @ConditionalOnMissingBean(name = "defaultRetryPolicy")
    public ResolvedRetryPolicy defaultRetryPolicy(EventFlowProperties properties) {
        ProcessingRetryConfig processing = properties.getDispatcher().getLifecycle().getRetry();
        return toResolvedPolicy(processing, processing.isEnabled());
    }

    /**
     * Resolves and registers named policies for listeners.
     *
     * @param registry      the named policy registry
     * @param defaultPolicy the configured default policy
     * @param properties    event-flow properties
     * @return the listener policy resolver
     */
    @Bean
    @ConditionalOnMissingBean
    public ListenerRetryPolicyResolver listenerRetryPolicyResolver(
            RetryPolicyRegistry registry,
            @Qualifier("defaultRetryPolicy") ResolvedRetryPolicy defaultPolicy,
            EventFlowProperties properties) {
        ProcessingRetryConfig processing = properties.getDispatcher().getLifecycle().getRetry();
        Map<String, RetryPolicyOverride> overrides =
                toOverrides(processing.getPolicies(), processing.isEnabled());
        return new ListenerRetryPolicyResolver(registry, defaultPolicy, overrides);
    }

    /**
     * Non-blocking scheduler used between local retry attempts.
     *
     * @return the default retry delay scheduler
     */
    @Bean
    @ConditionalOnMissingBean
    public RetryDelayScheduler retryDelayScheduler() {
        return RetryDelayScheduler.defaultScheduler();
    }

    private static ResolvedRetryPolicy toResolvedPolicy(ProcessingRetryConfig config, boolean enabled) {
        return new ResolvedRetryPolicy(
                ResolvedRetryPolicy.DEFAULT_POLICY_NAME,
                resolveTypes(config.getRetryOn()),
                resolveTypes(config.getNoRetryOn()),
                enabled ? config.getTotalRetries() : 0,
                enabled ? config.getLocalMaxRetries() : 0,
                config.getInitialDelay(),
                config.getMultiplier(),
                config.getMaxDelay(),
                config.getJitter());
    }

    private static Map<String, RetryPolicyOverride> toOverrides(
            Map<String, RetryPolicyOverrideConfig> configs, boolean enabled) {
        return configs.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> toOverride(entry.getValue(), enabled)));
    }

    private static RetryPolicyOverride toOverride(RetryPolicyOverrideConfig config, boolean enabled) {
        Integer maxRetries = enabled ? config.getTotalRetries() : 0;
        Integer localMaxRetries = enabled ? config.getLocalMaxRetries() : 0;
        return new RetryPolicyOverride(
                maxRetries,
                localMaxRetries,
                config.getInitialDelay(),
                config.getMultiplier(),
                config.getMaxDelay(),
                config.getJitter(),
                config.getRetryOn() != null ? resolveTypes(config.getRetryOn()) : null,
                config.getNoRetryOn() != null ? resolveTypes(config.getNoRetryOn()) : null);
    }

    private static Set<Class<? extends Throwable>> resolveTypes(List<String> names) {
        if (names == null || names.isEmpty()) {
            return Set.of();
        }
        Set<Class<? extends Throwable>> types = new LinkedHashSet<>();
        for (String name : names) {
            types.add(resolveType(name));
        }
        return Set.copyOf(types);
    }

    private static Class<? extends Throwable> resolveType(String name) {
        try {
            Class<?> type = ClassUtils.forName(name, RetryPolicyConfiguration.class.getClassLoader());
            if (!Throwable.class.isAssignableFrom(type)) {
                throw new IllegalArgumentException("Retry policy type is not a Throwable: " + name);
            }
            return type.asSubclass(Throwable.class);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Unknown retry policy exception type: " + name, e);
        }
    }
}
