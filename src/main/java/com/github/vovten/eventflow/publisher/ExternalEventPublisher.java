package com.github.vovten.eventflow.publisher;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventBus;

/**
 * Publishes events to the external bus
 *
 * @author Vladimir Aleshkov, 20.11.2024.
 */
@Component
@ConditionalOnExpression("${event.external.publisher.enabled:false} or ${event.replicas.dispatch.enabled:false}")
public class ExternalEventPublisher implements EventPublisher {

    private final String topic;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public ExternalEventPublisher(@Autowired KafkaTemplate<String, String> kafkaTemplate,
                                  @Value("${event.external.publisher.topic:depository.find.io-events}") String topic) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(Event event) {
        if (!event.eventBusTypes().contains(EventBus.EXTERNAL)) {
            throw new IllegalArgumentException("This publisher only supports EXTERNAL event bus");
        }
        kafkaTemplate.send(topic, event.asJson());
    }

    @Override
    public EventBus eventBus() {
        return EventBus.EXTERNAL;
    }
}
