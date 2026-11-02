package io.github.vovten.eventflow.retry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("RetryPolicyRegistry Tests")
class RetryPolicyRegistryTest {

    private final RetryPolicyRegistry registry = new RetryPolicyRegistry();

    @Test
    @DisplayName("Should register and find a named policy")
    void shouldRegisterAndFindPolicy() {
        ResolvedRetryPolicy policy = namedPolicy("payment");

        registry.register(policy);

        assertThat(registry.contains("payment")).isTrue();
        assertThat(registry.find("payment")).contains(policy);
        assertThat(registry.resolve("payment")).isSameAs(policy);
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should reject a duplicate policy name")
    void shouldRejectDuplicateName() {
        registry.register(namedPolicy("payment"));

        assertThatThrownBy(() -> registry.register(namedPolicy("payment")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate retry policy");
    }

    @Test
    @DisplayName("Should reject a policy without a name")
    void shouldRejectUnnamedPolicy() {
        assertThatThrownBy(() -> registry.register(policy(null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-blank name");

        assertThatThrownBy(() -> registry.register(policy("  ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-blank name");
    }

    @Test
    @DisplayName("Should report unknown policies")
    void shouldReportUnknownPolicy() {
        assertThat(registry.find("missing")).isEmpty();
        assertThat(registry.contains("missing")).isFalse();
        assertThatThrownBy(() -> registry.resolve("missing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown retry policy");
    }

    private static ResolvedRetryPolicy namedPolicy(String name) {
        return policy(name);
    }

    private static ResolvedRetryPolicy policy(String name) {
        return new ResolvedRetryPolicy(name, Set.of(), Set.of(), 3, 0,
                Duration.ofMillis(100), 2.0, Duration.ofSeconds(5), 0.5);
    }
}
