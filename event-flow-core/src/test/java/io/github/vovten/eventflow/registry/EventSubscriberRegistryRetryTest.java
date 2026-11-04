package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.EventSubscriber;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.retry.ListenerRetryPolicyResolver;
import io.github.vovten.eventflow.retry.ResolvedRetryPolicy;
import io.github.vovten.eventflow.retry.RetryPolicyRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("EventSubscriberRegistry Retry Policy Tests")
class EventSubscriberRegistryRetryTest {

    private RetryPolicyRegistry policyRegistry;
    private ResolvedRetryPolicy defaultPolicy;
    private ListenerRetryPolicyResolver resolver;
    private EventSubscriberRegistry registry;

    @BeforeEach
    void setUp() {
        policyRegistry = new RetryPolicyRegistry();
        defaultPolicy = ResolvedRetryPolicy.defaults();
        resolver = new ListenerRetryPolicyResolver(policyRegistry, defaultPolicy);
        registry = new EventSubscriberRegistry(resolver);
        policyRegistry.register(new ResolvedRetryPolicy(
                "shared", Set.of(), Set.of(), 3, 0,
                Duration.ofMillis(10), 2.0, Duration.ofSeconds(1), 0.0));
    }

    @Test
    @DisplayName("Should resolve a named policy for a subscriber")
    void shouldResolveNamedPolicyForSubscriber() {
        registry.register(new NamedSubscriber());

        EventHandler handler = registry.getHandlers(new TestEvent()).get(0);

        assertThat(registry.retryPolicyFor(handler).name()).isEqualTo("shared");
    }

    @Test
    @DisplayName("Should use the default policy when the reference is empty")
    void shouldUseDefaultPolicyWhenReferenceIsEmpty() {
        registry.register(new DefaultSubscriber());

        EventHandler handler = registry.getHandlers(new TestEvent()).get(0);

        assertThat(registry.retryPolicyFor(handler)).isSameAs(defaultPolicy);
    }

    @Test
    @DisplayName("Should reject an unknown policy reference")
    void shouldRejectUnknownPolicyReference() {
        assertThatThrownBy(() -> registry.register(new UnknownSubscriber()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown retry policy");
    }

    @Test
    @DisplayName("Should expose the subscriber policy through a composite registry")
    void shouldExposePolicyThroughCompositeRegistry() {
        EventListenerRegistry annotationRegistry = new EventListenerRegistry(resolver);
        CompositeEventHandlerRegistry composite = new CompositeEventHandlerRegistry(
                List.of(annotationRegistry, registry));
        registry.register(new NamedSubscriber());
        EventHandler handler = registry.getHandlers(new TestEvent()).get(0);

        assertThat(composite.retryPolicyFor(handler).name()).isEqualTo("shared");
    }

    private static final class TestEvent extends AbstractTraceableEvent {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }

    private static class DefaultSubscriber implements EventSubscriber {

        @Override
        public List<Class<?>> events() {
            return List.of(TestEvent.class);
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    private static final class NamedSubscriber extends DefaultSubscriber {

        @Override
        public String retryPolicy() {
            return "shared";
        }
    }

    private static final class UnknownSubscriber extends DefaultSubscriber {

        @Override
        public String retryPolicy() {
            return "missing";
        }
    }
}
