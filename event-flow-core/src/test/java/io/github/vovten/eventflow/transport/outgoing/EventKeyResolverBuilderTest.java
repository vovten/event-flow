package io.github.vovten.eventflow.transport.outgoing;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link EventKeyResolverBuilder}.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("EventKeyResolverBuilder Tests")
class EventKeyResolverBuilderTest {

    @Test
    @DisplayName("Should build none resolver for null, blank and 'none' specs")
    void shouldBuildNoneResolverForEmptyAndNoneSpecs() {
        UUID processId = UUID.randomUUID();
        Envelope<String> envelope = Envelope.of("payload", processId);

        for (String spec : new String[]{null, "", "   ", "none", "NONE"}) {
            assertThat(new EventKeyResolverBuilder(spec).build().resolveKey(envelope))
                    .as("spec '%s'", spec)
                    .isNull();
        }
    }

    @Test
    @DisplayName("Should build process-id resolver, case-insensitive")
    void shouldBuildProcessIdResolver() {
        UUID processId = UUID.randomUUID();
        Envelope<String> envelope = Envelope.of("payload", processId);

        for (String spec : new String[]{"process-id", "PROCESS-ID", "Process-Id"}) {
            assertThat(new EventKeyResolverBuilder(spec).build().resolveKey(envelope))
                    .as("spec '%s'", spec)
                    .isEqualTo(processId.toString());
        }
    }

    @Test
    @DisplayName("Should build event-type resolver")
    void shouldBuildEventTypeResolver() {
        TestEvent event = TestEvent.create("test-id", "test-message");

        EventKeyResolver resolver = new EventKeyResolverBuilder("event-type").build();

        assertThat(resolver.resolveKey(event)).isEqualTo(TestEvent.class.getName());
    }

    @Test
    @DisplayName("Should build metadata resolver with the named entry")
    void shouldBuildMetadataResolver() {
        Envelope<String> envelope = Envelope.of("payload").withAdditionalMetadata("orderId", "order-123");

        EventKeyResolver resolver = new EventKeyResolverBuilder("metadata:orderId").build();

        assertThat(resolver.resolveKey(envelope)).isEqualTo("order-123");
        assertThat(resolver.resolveKey(TestEvent.create("test-id", "msg"))).isNull();
    }

    @Test
    @DisplayName("Should reject metadata spec without a name")
    void shouldRejectMetadataWithoutName() {
        assertThatThrownBy(() -> new EventKeyResolverBuilder("metadata:").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a metadata name");
    }

    @Test
    @DisplayName("Should resolve bean spec through the configured lookup")
    void shouldResolveBeanSpecThroughLookup() {
        EventKeyResolver registered = EventKeyResolver.processId();
        Function<String, EventKeyResolver> registry = Map.of("orderKeyResolver", registered)::get;

        EventKeyResolver resolver = new EventKeyResolverBuilder("bean:orderKeyResolver")
                .withBeanLookup(registry)
                .build();

        assertThat(resolver).isSameAs(registered);
    }

    @Test
    @DisplayName("Should reject bean spec when no lookup is configured")
    void shouldRejectBeanWithoutLookup() {
        assertThatThrownBy(() -> new EventKeyResolverBuilder("bean:orderKeyResolver").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a bean lookup");
    }

    @Test
    @DisplayName("Should reject bean spec when lookup returns no resolver")
    void shouldRejectBeanWhenLookupMisses() {
        assertThatThrownBy(() -> new EventKeyResolverBuilder("bean:absentResolver")
                .withBeanLookup(name -> null)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("references unknown EventKeyResolver bean 'absentResolver'");
    }

    @Test
    @DisplayName("Should reject bean spec without a name")
    void shouldRejectBeanWithoutName() {
        assertThatThrownBy(() -> new EventKeyResolverBuilder("bean:").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a bean name");
    }

    @Test
    @DisplayName("Should reject unknown spec listing supported values")
    void shouldRejectUnknownSpec() {
        assertThatThrownBy(() -> new EventKeyResolverBuilder("partition-key").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown Kafka 'key' strategy 'partition-key'")
                .hasMessageContaining("metadata:<name>");
    }

    @Test
    @DisplayName("Should reject null lookup")
    void shouldRejectNullLookup() {
        assertThatNullPointerException()
                .isThrownBy(() -> new EventKeyResolverBuilder("none").withBeanLookup(null))
                .withMessageContaining("beanLookup must not be null");
    }
}
