package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.EventBus;
import com.github.vovten.eventflow.test.CompositeTestEvent;
import com.github.vovten.eventflow.test.ExternalTestEvent;
import com.github.vovten.eventflow.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for CompositeEventPublisher
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CompositeEventPublisherTest {

    @Mock
    private EventPublisher internalPublisher;

    @Mock
    private EventPublisher externalPublisher;

    private Map<EventBus, EventPublisher> eventPublishers;
    private CompositeEventPublisher compositePublisher;

    @BeforeEach
    void setUp() {
        eventPublishers = new HashMap<>();
        eventPublishers.put(EventBus.INTERNAL, internalPublisher);
        eventPublishers.put(EventBus.EXTERNAL, externalPublisher);
        compositePublisher = new CompositeEventPublisher(eventPublishers, false);
    }

    @Test
    @DisplayName("Should publish event to INTERNAL bus")
    void shouldPublishEventToInternalBus() {
        // given
        TestEvent event = TestEvent.create();

        // when
        compositePublisher.publish(event);

        // then
        verify(internalPublisher).publish(event);
        verify(externalPublisher, never()).publish(any());
    }

    @Test
    @DisplayName("Should publish event to EXTERNAL bus")
    void shouldPublishEventToExternalBus() {
        // given
        ExternalTestEvent event = ExternalTestEvent.create();

        // when
        compositePublisher.publish(event);

        // then
        verify(externalPublisher).publish(event);
        verify(internalPublisher, never()).publish(any());
    }

    @Test
    @DisplayName("Should publish event to both INTERNAL and EXTERNAL buses")
    void shouldPublishEventToBothBuses() {
        // given
        CompositeTestEvent event = CompositeTestEvent.create();

        // when
        compositePublisher.publish(event);

        // then
        verify(internalPublisher).publish(event);
        verify(externalPublisher).publish(event);
    }

    @Test
    @DisplayName("Should throw exception when no publisher found for bus")
    void shouldThrowExceptionWhenNoPublisherFound() {
        // given
        Map<EventBus, EventPublisher> publishers = new HashMap<>();
        // Only INTERNAL publisher, no EXTERNAL
        publishers.put(EventBus.INTERNAL, internalPublisher);
        CompositeEventPublisher publisher = new CompositeEventPublisher(publishers, false);
        
        ExternalTestEvent event = ExternalTestEvent.create();

        // when & then
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> publisher.publish(event)
        );
        assertTrue(exception.getMessage().contains("No publisher found for bus"));
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for eventBus() method")
    void shouldThrowUnsupportedOperationExceptionForEventBusMethod() {
        // when & then
        assertThrows(
            UnsupportedOperationException.class,
            () -> compositePublisher.eventBus()
        );
    }

    @Test
    @DisplayName("Should defer publishing when transaction is active and transactional publishing is enabled")
    void shouldDeferPublishingWhenTransactionIsActive() {
        // given
        CompositeEventPublisher transactionalPublisher = 
            new CompositeEventPublisher(eventPublishers, true);
        TestEvent event = TestEvent.create();
        
        // Simulate active transaction
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);

        try {
            // when
            transactionalPublisher.publish(event);

            // then - should not publish immediately
            verify(internalPublisher, never()).publish(event);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    @DisplayName("Should publish immediately when no transaction is active")
    void shouldPublishImmediatelyWhenNoTransaction() {
        // given
        CompositeEventPublisher transactionalPublisher = 
            new CompositeEventPublisher(eventPublishers, true);
        TestEvent event = TestEvent.create();

        // when
        transactionalPublisher.publish(event);

        // then
        verify(internalPublisher).publish(event);
    }
}
