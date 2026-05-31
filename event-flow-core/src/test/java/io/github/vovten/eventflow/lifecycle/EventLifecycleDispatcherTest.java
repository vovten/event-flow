package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.dispatcher.EventDispatcher;
import io.github.vovten.eventflow.dispatcher.HandlerResult;
import io.github.vovten.eventflow.dispatcher.HandlerResults;
import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("EventLifecycleDispatcher Tests")
class EventLifecycleDispatcherTest {

    private AtomicReference<Event> publishedAck;
    private EventPublisher ackPublisher;
    private TestEvent event;

    @BeforeEach
    void setUp() {
        event = new TestEvent("test");
        publishedAck = new AtomicReference<>();
        ackPublisher = ack -> {
            publishedAck.set(ack);
            return CompletableFuture.completedFuture(SendResults.of(List.of(SendResult.success("dest"))));
        };
    }

    @Test
    @DisplayName("Should publish SuccessAck on successful dispatch")
    void shouldPublishHandledOnSuccess() {
        EventDispatcher origin = successDispatcher();
        EventLifecycleDispatcher dispatcher = new EventLifecycleDispatcher(origin, ackPublisher);

        dispatcher.dispatch(event).join();

        assertThat(publishedAck.get()).isInstanceOf(SuccessAck.class);
        SuccessAck ack = (SuccessAck) publishedAck.get();
        assertThat(ack.originalEventId()).isEqualTo(event.eventId());
    }

    @Test
    @DisplayName("Should publish FailureAck on dispatch failure")
    void shouldPublishHandleFailedOnFailure() {
        EventDispatcher origin = failureDispatcher(new RuntimeException("Handler error"));
        EventLifecycleDispatcher dispatcher = new EventLifecycleDispatcher(origin, ackPublisher);

        dispatcher.dispatch(event).join();

        assertThat(publishedAck.get()).isInstanceOf(FailureAck.class);
        FailureAck ack = (FailureAck) publishedAck.get();
        assertThat(ack.originalEventId()).isEqualTo(event.eventId());
        assertThat(ack.error()).contains("Handler error");
    }

    @Test
    @DisplayName("Should not publish ack for LifecycleAckEvent")
    void shouldNotPublishAckForAckEvents() {
        EventDispatcher origin = successDispatcher();
        EventLifecycleDispatcher dispatcher = new EventLifecycleDispatcher(origin, ackPublisher);
        SuccessAck ackEvent = new SuccessAck(
                UUID.randomUUID(), event.eventId(), "TestEvent", null,
                List.of(), null, java.time.Instant.now());

        dispatcher.dispatch(ackEvent).join();

        assertThat(publishedAck.get()).isNull();
    }

    @Test
    @DisplayName("Should pass through non-TraceableEvent without publishing ack")
    void shouldPassThroughNonTraceableEvent() {
        EventDispatcher origin = successDispatcher();
        EventLifecycleDispatcher dispatcher = new EventLifecycleDispatcher(origin, ackPublisher);
        Event plainEvent = new Event() {
            @Override
            public Class<?> type() {
                return Event.class;
            }
        };

        dispatcher.dispatch(plainEvent).join();

        assertThat(publishedAck.get()).isNull();
    }

    @Test
    @DisplayName("Should not publish ack for event without MANAGED lifecycle")
    void shouldNotPublishAckForNonManagedLifecycle() {
        EventDispatcher origin = successDispatcher();
        EventLifecycleDispatcher dispatcher = new EventLifecycleDispatcher(origin, ackPublisher);
        Event nonManagedEvent = new Event() {
            @Override
            public Class<?> type() {
                return Event.class;
            }
        };

        dispatcher.dispatch(nonManagedEvent).join();

        assertThat(publishedAck.get()).isNull();
    }

    @Test
    @DisplayName("Should delegate all dispatcher methods")
    void shouldDelegateMethods() {
        EventDispatcher origin = successDispatcher();
        EventLifecycleDispatcher dispatcher = new EventLifecycleDispatcher(origin, ackPublisher);

        dispatcher.register(new Object());
        dispatcher.isRegistered(new Object());
        dispatcher.start(e -> {});
        dispatcher.stop();
    }

    private static EventDispatcher successDispatcher() {
        return new TestDispatcher(e -> CompletableFuture.completedFuture(
                HandlerResults.of(List.of(HandlerResult.success("test-handler")))));
    }

    private static EventDispatcher failureDispatcher(Throwable error) {
        return new TestDispatcher(e -> CompletableFuture.completedFuture(
                HandlerResults.of(List.of(
                        HandlerResult.failure("test-handler", error)))));
    }

    private static class TestDispatcher implements EventDispatcher {
        private final Function<Event, CompletableFuture<HandlerResults>> dispatchFn;

        TestDispatcher(Function<Event, CompletableFuture<HandlerResults>> dispatchFn) {
            this.dispatchFn = dispatchFn;
        }

        @Override
        public CompletableFuture<HandlerResults> dispatch(Event event) {
            return dispatchFn.apply(event);
        }

        @Override
        public void register(Object listener) {
        }

        @Override
        public boolean isRegistered(Object listener) {
            return false;
        }

        @Override
        public void start(Consumer<Event> dispatchConsumer) {
        }

        @Override
        public void stop() {
        }
    }

    @io.github.vovten.eventflow.event.annotation.Event(lifecycle = EventLifecycle.MANAGED)
    private static class TestEvent extends AbstractTraceableEvent {
        private final String data;

        TestEvent(String data) {
            super();
            this.data = data;
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }
}
