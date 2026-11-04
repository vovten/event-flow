package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.EventListener;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.registry.EventListenerRegistry.MethodInvokingEventHandler;
import io.github.vovten.eventflow.retry.ResolvedRetryPolicy;
import io.github.vovten.eventflow.retry.RetryPolicy;
import io.github.vovten.eventflow.retry.RetryPolicyRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("EventListenerRegistry Retry Policy Tests")
class EventListenerRegistryRetryTest {

    private RetryPolicyRegistry policyRegistry;
    private ResolvedRetryPolicy defaultPolicy;
    private EventListenerRegistry registry;

    @BeforeEach
    void setUp() {
        policyRegistry = new RetryPolicyRegistry();
        defaultPolicy = ResolvedRetryPolicy.defaults();
        registry = new EventListenerRegistry(policyRegistry, defaultPolicy);
    }

    @Test
    @DisplayName("Should register a method declaration and reuse it by name")
    void shouldRegisterMethodDeclarationAndReuseIt() {
        registry.register(new MethodDeclaredListener());
        registry.register(new ReferenceListener());

        MethodInvokingEventHandler declared = handlerFor(new OrderEvent());
        MethodInvokingEventHandler referenced = handlerFor(new PaymentEvent());

        assertThat(declared.resolvedRetryPolicy().name()).isEqualTo("order-retry");
        assertThat(referenced.resolvedRetryPolicy()).isSameAs(declared.resolvedRetryPolicy());
    }

    @Test
    @DisplayName("Should register a dedicated policy holder type")
    void shouldRegisterDedicatedPolicyType() {
        registry.registerPolicyType(SharedPolicies.class);
        registry.register(new SharedReferenceListener());

        assertThat(policyRegistry.contains("shared")).isTrue();
        assertThat(handlerFor(new RefundEvent()).resolvedRetryPolicy().name()).isEqualTo("shared");
    }

    @Test
    @DisplayName("Should not bind a type-level declaration implicitly")
    void shouldNotBindTypeLevelDeclarationImplicitly() {
        registry.register(new TypeAnnotatedListener());

        assertThat(policyRegistry.contains("listener-class-policy")).isTrue();
        assertThat(handlerFor(new RefundEvent()).resolvedRetryPolicy()).isSameAs(defaultPolicy);
    }

    @Test
    @DisplayName("Should use the default policy when nothing is configured")
    void shouldUseDefaultPolicyWhenNothingIsConfigured() {
        registry.register(new DefaultListener());

        assertThat(handlerFor(new OrderEvent()).resolvedRetryPolicy()).isSameAs(defaultPolicy);
    }

    @Test
    @DisplayName("Should reject a declaration combined with a reference")
    void shouldRejectDeclarationCombinedWithReference() {
        registry.registerPolicyType(SharedPolicies.class);

        assertThatThrownBy(() -> registry.register(new AmbiguousListener()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both");
    }

    @Test
    @DisplayName("Should reject an unknown named reference")
    void shouldRejectUnknownNamedReference() {
        assertThatThrownBy(() -> registry.register(new UnknownReferenceListener()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown retry policy");
    }

    @Test
    @DisplayName("Should reject duplicate declaration names with different policies")
    void shouldRejectDuplicateDeclarationNames() {
        assertThatThrownBy(() -> registry.register(new DuplicateNameListener()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate retry policy");
    }

    private MethodInvokingEventHandler handlerFor(Event event) {
        return registry.getHandlers(event).stream()
                .filter(MethodInvokingEventHandler.class::isInstance)
                .map(MethodInvokingEventHandler.class::cast)
                .findFirst()
                .orElseThrow();
    }

    private static final class OrderEvent implements Event {
        @Override
        public Class<?> type() {
            return OrderEvent.class;
        }
    }

    private static final class PaymentEvent implements Event {
        @Override
        public Class<?> type() {
            return PaymentEvent.class;
        }
    }

    private static final class RefundEvent implements Event {
        @Override
        public Class<?> type() {
            return RefundEvent.class;
        }
    }

    private static final class MethodDeclaredListener {

        @EventListener(OrderEvent.class)
        @RetryPolicy(name = "order-retry", maxRetries = 4, localMaxRetries = 1,
                initialDelay = "50ms", jitter = 0.0)
        public void on(OrderEvent event) {
        }
    }

    private static final class ReferenceListener {

        @EventListener(value = PaymentEvent.class, retryPolicy = "order-retry")
        public void on(PaymentEvent event) {
        }
    }

    @RetryPolicy(name = "shared", maxRetries = 2, localMaxRetries = 0,
            initialDelay = "10ms", jitter = 0.0)
    private static final class SharedPolicies {
    }

    private static final class SharedReferenceListener {

        @EventListener(value = RefundEvent.class, retryPolicy = "shared")
        public void on(RefundEvent event) {
        }
    }

    @RetryPolicy(name = "listener-class-policy", maxRetries = 1, localMaxRetries = 0,
            initialDelay = "10ms", jitter = 0.0)
    private static final class TypeAnnotatedListener {

        @EventListener(RefundEvent.class)
        public void on(RefundEvent event) {
        }
    }

    private static final class DefaultListener {

        @EventListener(OrderEvent.class)
        public void on(OrderEvent event) {
        }
    }

    private static final class AmbiguousListener {

        @EventListener(value = OrderEvent.class, retryPolicy = "shared")
        @RetryPolicy(name = "ambiguous", maxRetries = 1,
                initialDelay = "10ms", jitter = 0.0)
        public void on(OrderEvent event) {
        }
    }

    private static final class UnknownReferenceListener {

        @EventListener(value = OrderEvent.class, retryPolicy = "missing")
        public void on(OrderEvent event) {
        }
    }

    private static final class DuplicateNameListener {

        @EventListener(OrderEvent.class)
        @RetryPolicy(name = "duplicate", maxRetries = 1,
                initialDelay = "10ms", jitter = 0.0)
        public void onOrder(OrderEvent event) {
        }

        @EventListener(PaymentEvent.class)
        @RetryPolicy(name = "duplicate", maxRetries = 2,
                initialDelay = "10ms", jitter = 0.0)
        public void onPayment(PaymentEvent event) {
        }
    }
}
