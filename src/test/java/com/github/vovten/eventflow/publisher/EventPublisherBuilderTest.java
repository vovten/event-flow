package com.github.vovten.eventflow.publisher;

import com.github.vovten.eventflow.channel.EventChannel;
import com.github.vovten.eventflow.channel.ExternalEventChannel;
import com.github.vovten.eventflow.channel.InternalEventChannel;
import com.github.vovten.eventflow.test.TestEvent;
import com.github.vovten.eventflow.transport.outgoing.InMemoryOutgoingEventTransport;
import com.github.vovten.eventflow.transport.outgoing.KafkaOutgoingEventTransport;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

/**
 * Test for {@link EventPublisher}.
 */
@Slf4j
class EventPublisherBuilderTest {

    @Test
    void build() {
        List<EventChannel> channels = List.of(
                new InternalEventChannel(new InMemoryOutgoingEventTransport()),
                new ExternalEventChannel(new KafkaOutgoingEventTransport("localhost:9081", "topic"))
        );
        EventPublisher eventPublisher = EventPublisherBuilder
                .channels(channels)
                .withDecorator(publisher -> {
                    log.info("Message from custom decorator");
                    return publisher;})
                .transactional()
                .retryable(1, Duration.ofSeconds(1), 1.2)
                .silent()
                .build();
        Assertions.assertDoesNotThrow(() -> eventPublisher.publish(new TestEvent("id", "message")));
    }
}