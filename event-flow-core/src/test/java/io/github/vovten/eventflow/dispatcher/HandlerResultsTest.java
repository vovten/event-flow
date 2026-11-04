package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.retry.FailureKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("HandlerResults Tests")
class HandlerResultsTest {

    @Test
    @DisplayName("Should detect a retryable failure")
    void shouldDetectRetryableFailure() {
        HandlerResults results = HandlerResults.of(List.of(
                HandlerResult.success("ok"),
                HandlerResult.failure("bad", new RuntimeException("boom"),
                        "boom", FailureKind.TRANSIENT_EXHAUSTED, true, 3, "policy", 500L)));

        assertThat(results.hasRetryableFailure()).isTrue();
        assertThat(results.hasOnlyPermanentFailures()).isFalse();
    }

    @Test
    @DisplayName("Should detect only permanent failures")
    void shouldDetectOnlyPermanentFailures() {
        HandlerResults results = HandlerResults.of(List.of(
                HandlerResult.success("ok"),
                HandlerResult.failure("bad", new RuntimeException("boom"),
                        "boom", FailureKind.PERMANENT, false, 2, null, 0L)));

        assertThat(results.hasRetryableFailure()).isFalse();
        assertThat(results.hasOnlyPermanentFailures()).isTrue();
    }

    @Test
    @DisplayName("Should not report only permanent failures without failures")
    void shouldNotReportOnlyPermanentFailuresWithoutFailures() {
        HandlerResults results = HandlerResults.of(List.of(HandlerResult.success("ok")));

        assertThat(results.hasOnlyPermanentFailures()).isFalse();
        assertThat(results.hasRetryableFailure()).isFalse();
    }

    @Test
    @DisplayName("Should aggregate failed handler names")
    void shouldAggregateFailedHandlerNames() {
        HandlerResults results = HandlerResults.of(List.of(
                HandlerResult.success("ok"),
                HandlerResult.failure("first", "error-1"),
                HandlerResult.failure("second", "error-2")));

        assertThat(results.getFailedHandlerNames()).containsExactly("first", "second");
    }

    @Test
    @DisplayName("Should return the maximum retry delay")
    void shouldReturnMaximumRetryDelay() {
        HandlerResults results = HandlerResults.of(List.of(
                HandlerResult.failure("first", null, "error",
                        FailureKind.TRANSIENT_EXHAUSTED, true, 1, "first-policy", 100L),
                HandlerResult.failure("second", null, "error",
                        FailureKind.TRANSIENT_EXHAUSTED, true, 2, "second-policy", 500L)));

        assertThat(results.getMaxRetryAfterMs()).isEqualTo(500L);
    }

    @Test
    @DisplayName("Should return the policy name of the first retryable failure")
    void shouldReturnRetryPolicyName() {
        HandlerResults results = HandlerResults.of(List.of(
                HandlerResult.failure("permanent", null, "error",
                        FailureKind.PERMANENT, false, 1, "permanent-policy", 0L),
                HandlerResult.failure("retryable", null, "error",
                        FailureKind.TRANSIENT_EXHAUSTED, true, 2, "retryable-policy", 100L)));

        assertThat(results.getRetryPolicyName()).isEqualTo("retryable-policy");
    }

    @Test
    @DisplayName("Should return zero/null aggregates when there are no failures")
    void shouldReturnEmptyAggregatesWithoutFailures() {
        HandlerResults results = HandlerResults.of(List.of(HandlerResult.success("ok")));

        assertThat(results.getFailedHandlerNames()).isEmpty();
        assertThat(results.getMaxRetryAfterMs()).isZero();
        assertThat(results.getRetryPolicyName()).isNull();
    }
}
