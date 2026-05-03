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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
    @DisplayName("Should unwrap Envelope and dispatch payload to handlers when no Envelope handlers exist")
    void shouldUnwrapEnvelopeAndDispatchPayload() throws InterruptedException {
        PayloadEventSubscriber subscriber = new PayloadEventSubscriber();
        when(handlerRegistry.getHandlers(any())).thenAnswer(invocation -> {
            Event event = invocation.getArgument(0);
            if (event instanceof Envelope) {
                return List.of();
            }
            return List.of(subscriber);
        });

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport));

        PlainDomainEvent payload = new PlainDomainEvent("order-123", "test@mail.ru");
        Envelope<PlainDomainEvent> envelope = Envelope.of(payload, "trace-abc");
        dispatcher.dispatch(envelope);

        Thread.sleep(100);

        assertThat(subscriber.receivedEvent).isNotNull();
        assertThat(subscriber.receivedEvent).isInstanceOf(PlainDomainEvent.class);
        assertThat(((PlainDomainEvent) subscriber.receivedEvent).orderId()).isEqualTo("order-123");
    }

    @Test
    @DisplayName("Should dispatch Envelope directly when Envelope handlers exist")
    void shouldDispatchEnvelopeDirectlyWhenHandlersExist() throws InterruptedException {
        EnvelopeEventSubscriber subscriber = new EnvelopeEventSubscriber();
        when(handlerRegistry.getHandlers(any())).thenAnswer(invocation -> {
            Event event = invocation.getArgument(0);
            if (event instanceof Envelope) {
                return List.of(subscriber);
            }
            return List.of();
        });

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport));

        PlainDomainEvent payload = new PlainDomainEvent("order-456", "user@mail.ru");
        Envelope<PlainDomainEvent> envelope = Envelope.of(payload, "trace-xyz");
        dispatcher.dispatch(envelope);

        Thread.sleep(100);

        assertThat(subscriber.receivedEvent).isNotNull();
        assertThat(subscriber.receivedEvent).isInstanceOf(Envelope.class);
    }

    @Test
    @DisplayName("Should preserve metadata when Envelope is dispatched")
    void shouldPreserveMetadataWhenEnvelopeDispatched() throws InterruptedException {
        MetadataPreservingSubscriber subscriber = new MetadataPreservingSubscriber();
        when(handlerRegistry.getHandlers(any())).thenAnswer(invocation -> {
            Event event = invocation.getArgument(0);
            if (event instanceof Envelope) {
                return List.of(subscriber);
            }
            return List.of();
        });

        dispatcher = new UnifiedEventDispatcher(executorService, handlerRegistry, List.of(transport));

        PlainDomainEvent payload = new PlainDomainEvent("order-789", "admin@mail.ru");
        Envelope<PlainDomainEvent> envelope = Envelope.of(payload, "trace-metadata");
        dispatcher.dispatch(envelope);

        Thread.sleep(100);

        assertThat(subscriber.receivedEvent).isInstanceOf(Envelope.class);
        Envelope<?> receivedEnvelope = (Envelope<?>) subscriber.receivedEvent;
        assertThat(receivedEnvelope.traceId()).isEqualTo("trace-metadata");
        assertThat(receivedEnvelope.payload()).isInstanceOf(PlainDomainEvent.class);
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

    static class EnvelopeEventSubscriber implements EventSubscriber {
        volatile Event receivedEvent;

        @Override
        public List<Class<?>> events() {
            return List.of(Envelope.class);
        }

        @Override
        public void onEvent(Event event) {
            this.receivedEvent = event;
        }
    }

    static class MetadataPreservingSubscriber implements EventSubscriber {
        volatile Event receivedEvent;

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