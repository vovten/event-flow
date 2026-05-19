package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.EventSubscriber;
import io.github.vovten.eventflow.registry.EventHandlerRegistry;
import io.github.vovten.eventflow.transport.InTransport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @since 1.1.0
 */
@DisplayName("UnifiedEventDispatcher Envelope Handling Tests")
class UnifiedEventDispatcherEnvelopeTest {

    private ExecutorService executorService;
    private EventHandlerRegistry handlerRegistry;
    private InTransport transport;
    private UnifiedEventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);
        handlerRegistry = mock(EventHandlerRegistry.class);
        transport = mock(InTransport.class);
        when(transport.name()).thenReturn("test-transport");
    }

    @Test
    @DisplayName("Should pass Envelope through when payload implements Event")
    void shouldPassEnvelopeThroughWhenPayloadImplementsEvent() throws InterruptedException {
        PayloadEventSubscriber subscriber = new PayloadEventSubscriber();
        when(handlerRegistry.getHandlers(any())).thenAnswer(invocation -> {
            Object event = invocation.getArgument(0);
            // After CRIT-2 fix: event is the Envelope, not the unwrapped payload.
            // Real EventListenerRegistry handles this via resolveEventType(),
            // but with a mock we match the Envelope and unwrap in the handler.
            if (event instanceof Envelope<?> envelope) {
                if (envelope.payload() instanceof PlainDomainEvent) {
                    return List.of(subscriber);
                }
            }
            return List.of();
        });

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport));

        PlainDomainEvent payload = new PlainDomainEvent("order-123", "test@mail.ru");
        UUID processId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        Envelope<PlainDomainEvent> envelope = Envelope.of(payload, processId);
        dispatcher.dispatch(envelope);

        Thread.sleep(100);

        assertThat(subscriber.receivedEvent).isNotNull();
        // Handler receives the Envelope (not unwrapped)
        assertThat(subscriber.receivedEvent).isInstanceOf(Envelope.class);
        Envelope<?> receivedEnvelope = (Envelope<?>) subscriber.receivedEvent;
        assertThat(receivedEnvelope.payload()).isInstanceOf(PlainDomainEvent.class);
        assertThat(((PlainDomainEvent) receivedEnvelope.payload()).orderId()).isEqualTo("order-123");
    }

    @Test
    @DisplayName("Should dispatch Envelope when payload does not implement Event")
    void shouldDispatchEnvelopeWhenPayloadNotEvent() throws InterruptedException {
        NonEventPayload payload = new NonEventPayload("data-123");
        NonEventSubscriber subscriber = new NonEventSubscriber();
        when(handlerRegistry.getHandlers(any())).thenAnswer(invocation -> {
            Object event = invocation.getArgument(0);
            if (event instanceof Envelope) {
                return List.of(subscriber);
            }
            return List.of();
        });

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport));

        UUID processId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        Envelope<NonEventPayload> envelope = Envelope.of(payload, processId);
        dispatcher.dispatch(envelope);

        Thread.sleep(100);

        assertThat(subscriber.receivedEvent).isNotNull();
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

    static class PayloadEventSubscriber implements EventSubscriber {
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

    static class NonEventSubscriber implements EventSubscriber {
        volatile Object receivedEvent;

        @Override
        public List<Class<?>> events() {
            return List.of(Envelope.class);
        }

        @Override
        public void onEvent(Event event) {
            this.receivedEvent = event;
        }
    }
}