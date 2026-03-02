package com.github.vovten.eventflow.event.dispatcher;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import com.github.vovten.eventflow.event.Event;

import java.util.concurrent.ExecutorService;

/**
 * Event dispatcher that listens to events from the external bus (see {@link com.github.vovten.eventflow.event.EventBus#EXTERNAL})
 *
 * @author Vladimir Aleshkov, 21.11.2024.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "event.external.dispatcher.enabled", havingValue = "true")
public class ExternalEventDispatcher extends AbstractEventDispatcher {

    public ExternalEventDispatcher(ExecutorService executorService) {
        super(executorService);
    }

    @Autowired
    public ExternalEventDispatcher(ExecutorService executorService,
                                   @Value("${event.listener.scan.package:}") String eventListenerScanPackage) {
        super(executorService, eventListenerScanPackage);
    }

    @KafkaListener(topics = "#{'${event.external.dispatcher.topics}'.split(',')}",
                   containerFactory = "kafkaEventDispatcherContainerFactory")
    public void dispatch(Event event, @Header(KafkaHeaders.GROUP_ID) String groupId) {
        super.dispatch(event);
        log.debug("Event received by ExternalEventDispatcher: {}, groupId: {}", event.asJson(), groupId);
    }
}
