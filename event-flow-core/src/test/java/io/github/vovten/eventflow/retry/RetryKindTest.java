package io.github.vovten.eventflow.retry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("RetryKind Tests")
class RetryKindTest {

    @Test
    @DisplayName("Should declare exactly DELIVERY and PROCESSING in order")
    void shouldDeclareRetryKinds() {
        assertThat(RetryKind.values())
                .containsExactly(RetryKind.DELIVERY, RetryKind.PROCESSING);
    }

    @Test
    @DisplayName("Should resolve values by name")
    void shouldResolveByName() {
        assertThat(RetryKind.valueOf("DELIVERY")).isEqualTo(RetryKind.DELIVERY);
        assertThat(RetryKind.valueOf("PROCESSING")).isEqualTo(RetryKind.PROCESSING);
    }
}
