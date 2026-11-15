package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.event.AbstractTraceableEvent;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.publisher.EventPublisher;
import io.github.vovten.eventflow.transport.SendResult;
import io.github.vovten.eventflow.transport.SendResults;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Threading tests for {@link EventLifecyclePublisher}.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("EventLifecyclePublisher Threading Tests")
class EventLifecyclePublisherThreadingTest {

    @Test
    @DisplayName("DEFECT-3.5: Should update the store off the transport completion thread")
    void shouldNotRunStoreUpdatesOnTransportThread() throws Exception {
        EventStore store = mock(EventStore.class);
        when(store.findById(any())).thenReturn(Optional.empty());
        AtomicReference<String> updateThread = new AtomicReference<>();
        when(store.transitionStatus(any(), anySet(), any(), any())).thenAnswer(invocation -> {
            updateThread.set(Thread.currentThread().getName());
            return true;
        });

        CompletableFuture<SendResults> originFuture = new CompletableFuture<>();
        EventPublisher origin = event -> originFuture;
        EventLifecyclePublisher publisher = new EventLifecyclePublisher(origin, store, "test-service");

        // The lifecycle callback is attached during publish(); only afterwards is
        // the origin future completed, from the simulated transport IO thread.
        CompletableFuture<SendResults> result = publisher.publish(new TestEvent());
        Thread transportIo = new Thread(
                () -> originFuture.complete(SendResults.of(List.of(SendResult.success("dest")))),
                "origin-io");
        transportIo.start();
        result.join();
        transportIo.join();

        assertThat(updateThread.get())
                .as("A blocking JDBC update must not run on the transport's IO thread")
                .isNotNull()
                .isNotEqualTo("origin-io")
                .startsWith("event-flow-store-updater");
    }

    static class TestEvent extends AbstractTraceableEvent {

        TestEvent() {
            super();
        }

        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }

        @Override
        public EventLifecycle lifecycle() {
            return EventLifecycle.MANAGED;
        }
    }
}
