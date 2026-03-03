package com.github.vovten.eventflow.event.publisher;

import com.github.vovten.eventflow.event.EventBus;
import com.github.vovten.eventflow.event.test.ExternalTestEvent;
import com.github.vovten.eventflow.event.test.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ExternalEventPublisher
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ExternalEventPublisherTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private CompletableFuture<org.springframework.kafka.support.SendResult<String, String>> completableFuture;

    private ExternalTestEvent testEvent;
    private ExternalEventPublisher publisher;

    @BeforeEach
    void setUp() {
        testEvent = ExternalTestEvent.create("Test payload");
        publisher = new ExternalEventPublisher(kafkaTemplate, "test-topic");
    }

    @Test
    @DisplayName("Should publish event to Kafka topic")
    void shouldPublishEventToKafkaTopic() {
        // given
        when(kafkaTemplate.send(anyString(), anyString())).thenReturn(completableFuture);

        // when
        publisher.publish(testEvent);

        // then
        verify(kafkaTemplate).send(eq("test-topic"), anyString());
    }

    @Test
    @DisplayName("Should return EXTERNAL event bus")
    void shouldReturnExternalEventBus() {
        // when
        EventBus eventBus = publisher.eventBus();

        // then
        assertEquals(EventBus.EXTERNAL, eventBus);
    }

    @Test
    @DisplayName("Should throw exception when event does not support EXTERNAL bus")
    void shouldThrowExceptionWhenEventDoesNotSupportExternalBus() {
        // given
        TestEvent internalEvent = TestEvent.create();

        // when & then
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> publisher.publish(internalEvent)
        );
        assertTrue(exception.getMessage().contains("EXTERNAL"));
    }

    @Test
    @DisplayName("Should use default topic when not specified")
    void shouldUseDefaultTopicWhenNotSpecified() {
        // given
        ExternalEventPublisher defaultPublisher = new ExternalEventPublisher(kafkaTemplate, "depository.find.io-events");
        when(kafkaTemplate.send(anyString(), anyString())).thenReturn(completableFuture);

        // when
        defaultPublisher.publish(testEvent);

        // then
        verify(kafkaTemplate).send(eq("depository.find.io-events"), anyString());
    }

    @Test
    @DisplayName("Should serialize event to JSON before sending")
    void shouldSerializeEventToJsonBeforeSending() {
        // given
        ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
        when(kafkaTemplate.send(anyString(), anyString())).thenReturn(completableFuture);

        // when
        publisher.publish(testEvent);

        // then
        verify(kafkaTemplate).send(eq("test-topic"), jsonCaptor.capture());
        String json = jsonCaptor.getValue();
        assertNotNull(json);
        assertTrue(json.contains("Test payload"));
        assertTrue(json.contains("ExternalTestEvent"));
    }
}
