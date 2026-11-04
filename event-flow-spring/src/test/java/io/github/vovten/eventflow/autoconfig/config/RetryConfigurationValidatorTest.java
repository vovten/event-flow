package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("RetryConfigurationValidator Tests")
class RetryConfigurationValidatorTest {

    private final RetryConfigurationValidator validator = new RetryConfigurationValidator();

    @Test
    @DisplayName("should accept both retry domains disabled")
    void acceptsDisabledDomains() {
        EventFlowProperties properties = new EventFlowProperties();

        assertThatCode(() -> validator.validate(properties)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("should accept delivery-only mode")
    void acceptsDeliveryOnly() {
        EventFlowProperties properties = new EventFlowProperties();
        properties.getRetry().getDelivery().setEnabled(true);

        assertThatCode(() -> validator.validate(properties)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("should accept full two-domain mode")
    void acceptsFullMode() {
        EventFlowProperties properties = new EventFlowProperties();
        properties.getRetry().getDelivery().setEnabled(true);
        properties.getRetry().getProcessing().setDurableEnabled(true);

        assertThatCode(() -> validator.validate(properties)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("should reject durable processing without delivery")
    void rejectsProcessingWithoutDelivery() {
        EventFlowProperties properties = new EventFlowProperties();
        properties.getRetry().getProcessing().setDurableEnabled(true);

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("processing retries depend on the delivery boundary");
    }
}
