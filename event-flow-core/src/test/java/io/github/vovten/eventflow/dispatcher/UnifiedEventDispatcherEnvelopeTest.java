package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.EventSubscriber;
import io.github.vovten.eventflow.registry.EventHandlerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @since 1.1.0
 */
@DisplayName("UnifiedEventDispatcher Envelope Handling Tests")
class UnifiedEventDispatcherEnvelopeTest {

    private ExecutorService executorService;
    private EventHandlerRegistry handlerRegistry;
    private UnifiedEventDispatcher dispatcher;
    private Map<Class<?>, List<EventHandler>> handlerMap;

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        handlerMap = new HashMap<>();
        handlerRegistry = new MapBasedHandlerRegistry(handlerMap);
    }

    @Test
    @DisplayName("Should pass Envelope through when payload implements Event")
    void shouldPassEnvelopeThroughWhenPayloadImplementsEvent() {
        PayloadEventSubscriber subscriber = new PayloadEventSubscriber();
        handlerMap.put(PlainDomainEvent.class, List.of(subscriber));

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of());

        PlainDomainEvent payload = new PlainDomainEvent("order-123", "test@mail.ru");
        UUID processId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        Envelope<PlainDomainEvent> envelope = Envelope.of(payload, processId);
        dispatcher.dispatch(envelope);

        await().atMost(Duration.ofSeconds(2)).until(() -> subscriber.receivedEvent != null);
        // Handler receives the Envelope (not unwrapped)
        assertThat(subscriber.receivedEvent).isInstanceOf(Envelope.class);
        Envelope<?> receivedEnvelope = (Envelope<?>) subscriber.receivedEvent;
        assertThat(receivedEnvelope.payload()).isInstanceOf(PlainDomainEvent.class);
        assertThat(((PlainDomainEvent) receivedEnvelope.payload()).orderId()).isEqualTo("order-123");
    }

    @Test
    @DisplayName("Should dispatch Envelope when payload does not implement Event")
    void shouldDispatchEnvelopeWhenPayloadNotEvent() {
        NonEventPayload payload = new NonEventPayload("data-123");
        NonEventSubscriber subscriber = new NonEventSubscriber();
        handlerMap.put(NonEventPayload.class, List.of(subscriber));

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of());

        UUID processId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        Envelope<NonEventPayload> envelope = Envelope.of(payload, processId);
        dispatcher.dispatch(envelope);

        await().atMost(Duration.ofSeconds(2)).until(() -> subscriber.receivedEvent != null);
        assertThat(subscriber.receivedEvent).isInstanceOf(Envelope.class);
        Envelope<?> receivedEnvelope = (Envelope<?>) subscriber.receivedEvent;
        assertThat(receivedEnvelope.processId()).isEqualTo(processId);
        assertThat(receivedEnvelope.payload()).isInstanceOf(NonEventPayload.class);
    }

    static class PlainDomainEvent implements Event {
        private final String orderId;
        private final String email;

        PlainDomainEvent(String orderId, String email) {
            this.orderId = orderId;
            this.email = email;
        }

        String orderId() {
            return orderId;
        }

        String email() {
            return email;
        }

        @Override
        public Class<? extends Event> type() {
            return PlainDomainEvent.class;
        }
    }

    static class NonEventPayload {
        private final String data;

        NonEventPayload(String data) {
            this.data = data;
        }

        String data() {
            return data;
        }
    }

    static class PayloadEventSubscriber implements EventSubscriber, EventHandler {
        volatile Event receivedEvent;

        @Override
        public List<Class<?>> events() {
            return List.of(PlainDomainEvent.class);
        }

        @Override
        public void onEvent(Event event) {
            this.receivedEvent = event;
        }
    }

    static class NonEventSubscriber implements EventHandler {
        volatile Object receivedEvent;

        @Override
        public void onEvent(Event event) {
            this.receivedEvent = event;
        }
    }
}