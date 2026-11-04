package io.github.vovten.eventflow;

import io.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("EventListener Annotation Tests")
class EventListenerAnnotationTest {

    @Test
    @DisplayName("Should default retryPolicy to empty")
    void shouldDefaultRetryPolicyToEmpty() {
        EventListener annotation = annotationOf("defaultPolicy");

        assertThat(annotation.retryPolicy()).isEmpty();
    }

    @Test
    @DisplayName("Should expose a named retry policy reference")
    void shouldExposeRetryPolicyReference() {
        EventListener annotation = annotationOf("namedPolicy");

        assertThat(annotation.retryPolicy()).isEqualTo("order-retry");
    }

    @Test
    @DisplayName("Should keep the event type attribute")
    void shouldKeepEventTypeAttribute() {
        EventListener annotation = annotationOf("namedPolicy");

        assertThat(annotation.value()).isEqualTo(Event.class);
    }

    private static EventListener annotationOf(String methodName) {
        try {
            return Annotated.class.getDeclaredMethod(methodName, Event.class)
                    .getAnnotation(EventListener.class);
        } catch (NoSuchMethodException e) {
            throw new AssertionError("Missing annotated method: " + methodName, e);
        }
    }

    @SuppressWarnings("unused")
    private static final class Annotated {

        @EventListener
        private void defaultPolicy(Event event) {
        }

        @EventListener(value = Event.class, retryPolicy = "order-retry")
        private void namedPolicy(Event event) {
        }
    }
}
