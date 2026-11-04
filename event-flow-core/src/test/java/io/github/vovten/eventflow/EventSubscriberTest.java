package io.github.vovten.eventflow;

import io.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("EventSubscriber Tests")
class EventSubscriberTest {

    @Test
    @DisplayName("Should default retryPolicy reference to empty")
    void shouldDefaultRetryPolicyToEmpty() {
        EventSubscriber subscriber = new DefaultSubscriber();

        assertThat(subscriber.retryPolicy()).isEmpty();
    }

    @Test
    @DisplayName("Should allow a named retry policy reference")
    void shouldAllowNamedRetryPolicyReference() {
        EventSubscriber subscriber = new NamedSubscriber();

        assertThat(subscriber.retryPolicy()).isEqualTo("payment");
    }

    @Test
    @DisplayName("Should keep the default handler name")
    void shouldKeepDefaultHandlerName() {
        EventSubscriber subscriber = new DefaultSubscriber();

        assertThat(subscriber.name()).isEqualTo(DefaultSubscriber.class.getSimpleName());
    }

    private static class DefaultSubscriber implements EventSubscriber {

        @Override
        public List<Class<?>> events() {
            return List.of();
        }

        @Override
        public void onEvent(Event event) {
        }
    }

    private static final class NamedSubscriber extends DefaultSubscriber {

        @Override
        public String retryPolicy() {
            return "payment";
        }
    }
}
