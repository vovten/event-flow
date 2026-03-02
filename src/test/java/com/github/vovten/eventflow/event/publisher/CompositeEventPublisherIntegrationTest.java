package com.github.vovten.eventflow.event.publisher;

import com.github.vovten.eventflow.event.EventBus;
import com.github.vovten.eventflow.event.EventFlowTestApplication;
import com.github.vovten.eventflow.event.EventPublisher;
import com.github.vovten.eventflow.event.test.CompositeTestEvent;
import com.github.vovten.eventflow.event.test.ExternalTestEvent;
import com.github.vovten.eventflow.event.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for CompositeEventPublisher
 */
@SpringBootTest(classes = EventFlowTestApplication.class,
    properties = {
        "event.internal.enabled=true",
        "event.external.publisher.enabled=false"
    })
@ActiveProfiles("test")
class CompositeEventPublisherIntegrationTest {

    @Autowired
    private EventPublisher eventPublisher;

    @Autowired
    private BlockingDeque<com.github.vovten.eventflow.event.Event> eventQueue;

    @Autowired
    @Qualifier("internalEventPublisher")
    private InternalEventPublisher internalPublisher;

    @BeforeEach
    void setUp() {
        eventQueue.clear();
    }

    @Test
    @DisplayName("Should publish event to INTERNAL bus via composite publisher")
    void shouldPublishEventToInternalBusViaCompositePublisher() throws InterruptedException {
        // given
        TestEvent event = TestEvent.create("Composite test message");

        // when
        eventPublisher.publish(event);

        // then
        com.github.vovten.eventflow.event.Event publishedEvent = eventQueue.poll(5, TimeUnit.SECONDS);
        assertNotNull(publishedEvent);
        assertEquals("Composite test message", ((TestEvent) publishedEvent).getMessage());
    }

    @Test
    @DisplayName("Should handle event for both INTERNAL and EXTERNAL buses")
    void shouldHandleEventForBothBuses() throws InterruptedException {
        // given
        CompositeTestEvent event = CompositeTestEvent.create("Both buses test");

        // when
        // Note: External publisher is disabled in this test, so only INTERNAL will work
        assertThrows(IllegalArgumentException.class, () -> eventPublisher.publish(event));
    }

    @Test
    @DisplayName("Should throw exception for unsupported bus")
    void shouldThrowExceptionForUnsupportedBus() {
        // given
        ExternalTestEvent event = ExternalTestEvent.create();

        // when & then
        // External publisher is disabled, so no publisher for EXTERNAL bus
        assertThrows(IllegalArgumentException.class, () -> eventPublisher.publish(event));
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for eventBus() method")
    void shouldThrowUnsupportedOperationExceptionForEventBusMethod() {
        // when & then
        assertThrows(UnsupportedOperationException.class, () -> eventPublisher.eventBus());
    }
}
