package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties.DefaultRetryPolicyConfig;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties.ProcessingRetryConfig;
import io.github.vovten.eventflow.autoconfig.EventFlowProperties.RetryPolicyOverrideConfig;
import io.github.vovten.eventflow.retry.ListenerRetryPolicyResolver;
import io.github.vovten.eventflow.retry.ResolvedRetryPolicy;
import io.github.vovten.eventflow.retry.RetryPolicy;
import io.github.vovten.eventflow.retry.RetryPolicyRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("RetryPolicyConfiguration Tests")
class RetryPolicyConfigurationTest {

    private final RetryPolicyConfiguration configuration = new RetryPolicyConfiguration();

    @Test
    @DisplayName("Should build the default policy from properties")
    void shouldBuildDefaultPolicyFromProperties() {
        EventFlowProperties properties = properties();
        DefaultRetryPolicyConfig config = properties.getRetry().getProcessing().getDefaultPolicy();
        config.setMaxRetries(5);
        config.setLocalMaxRetries(2);
        config.setInitialDelay(Duration.ofSeconds(1));
        config.setMultiplier(3.0);
        config.setMaxDelay(Duration.ofSeconds(10));
        config.setJitter(0.0);
        config.setRetryOn(List.of(TimeoutException.class.getName()));

        ResolvedRetryPolicy policy = configuration.defaultRetryPolicy(properties);

        assertThat(policy.name()).isEqualTo(ResolvedRetryPolicy.DEFAULT_POLICY_NAME);
        assertThat(policy.maxRetries()).isEqualTo(5);
        assertThat(policy.localMaxRetries()).isEqualTo(2);
        assertThat(policy.initialDelay()).isEqualTo(Duration.ofSeconds(1));
        assertThat(policy.multiplier()).isEqualTo(3.0);
        assertThat(policy.maxDelay()).isEqualTo(Duration.ofSeconds(10));
        assertThat(policy.jitter()).isZero();
        assertThat(policy.retryOn()).containsExactly(TimeoutException.class);
    }

    @Test
    @DisplayName("Should apply named policy overrides")
    void shouldApplyNamedPolicyOverrides() {
        EventFlowProperties properties = properties();
        RetryPolicyOverrideConfig override = new RetryPolicyOverrideConfig();
        override.setMaxRetries(7);
        override.setLocalMaxRetries(1);
        properties.getRetry().getProcessing().getPolicies().put("payment", override);

        RetryPolicyRegistry registry = configuration.retryPolicyRegistry();
        ResolvedRetryPolicy defaultPolicy = configuration.defaultRetryPolicy(properties);
        ListenerRetryPolicyResolver resolver = configuration.listenerRetryPolicyResolver(
                registry, defaultPolicy, properties);

        resolver.prepare(PaymentPolicies.class);

        ResolvedRetryPolicy policy = registry.resolve("payment");
        assertThat(policy.maxRetries()).isEqualTo(7);
        assertThat(policy.localMaxRetries()).isEqualTo(1);
        assertThat(policy.initialDelay()).isEqualTo(Duration.ofMillis(10));
    }

    @Test
    @DisplayName("Should disable local retries when processing is disabled")
    void shouldDisableLocalRetriesWhenProcessingDisabled() {
        EventFlowProperties properties = properties();
        ProcessingRetryConfig processing = properties.getRetry().getProcessing();
        processing.setEnabled(false);
        processing.getDefaultPolicy().setLocalMaxRetries(5);

        ResolvedRetryPolicy policy = configuration.defaultRetryPolicy(properties);

        assertThat(policy.localMaxRetries()).isZero();
    }

    @Test
    @DisplayName("Should reject an unknown exception class")
    void shouldRejectUnknownExceptionClass() {
        EventFlowProperties properties = properties();
        properties.getRetry().getProcessing().getDefaultPolicy()
                .setRetryOn(List.of("com.example.MissingException"));

        assertThatThrownBy(() -> configuration.defaultRetryPolicy(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown retry policy exception type");
    }

    @Test
    @DisplayName("Should provide a default retry delay scheduler")
    void shouldProvideDefaultRetryDelayScheduler() {
        assertThat(configuration.retryDelayScheduler()).isNotNull();
    }

    private static EventFlowProperties properties() {
        return new EventFlowProperties();
    }

    @RetryPolicy(name = "payment", maxRetries = 2, localMaxRetries = 0,
            initialDelay = "10ms", jitter = 0.0)
    private static final class PaymentPolicies {
    }
}
