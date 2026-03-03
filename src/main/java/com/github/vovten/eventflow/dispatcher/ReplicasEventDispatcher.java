package com.github.vovten.eventflow.dispatcher;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import com.github.vovten.eventflow.Event;

import java.util.concurrent.ExecutorService;

/**
 * <h2>Event dispatcher for consuming events by each service replica</h2>
 *
 * <p>Implements a broadcast delivery mechanism for events to all service instances (pods) via Apache Kafka,
 * using unique consumer groups for each replica.</p>
 *
 * <h3>Activation conditions:</h3>
 * <p>Activated only when the configuration property {@code event.replicas.dispatcher.enabled=true} is present.</p>
 *
 * <h3>Workflow:</h3>
 * <pre>
 *     → [Replica 1 (group: svc-name-pod1)]
 *     → [Producer] → [Kafka Topic] → [Replica 2 (group: svc-name-pod2)]
 *     → [Replica N (group: svc-name-podN)]
 * </pre>
 *
 * <h3>Required configuration:</h3>
 * <table border="1">
 *     <tr><th>Property</th><th>Description</th><th>Required</th></tr>
 *     <tr><td>event.service.topic</td><td>Service topic</td><td>true</td></tr>
 *     <tr><td>spring.application.name</td><td>Service name</td><td>true</td></tr>
 * </table>
 *
 * @author Vladimir Aleshkov, 21.11.2024.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "event.replicas.dispatcher.enabled", havingValue = "true")
public class ReplicasEventDispatcher extends AbstractEventDispatcher {

    public ReplicasEventDispatcher(ExecutorService executorService) {
        super(executorService);
    }

    @Autowired
    public ReplicasEventDispatcher(ExecutorService executorService,
                                   @Value("${event.listener.scan.package:}") String eventListenerScanPackage) {
        super(executorService, eventListenerScanPackage);
    }

    @KafkaListener(topics = "${event.service.topic}",
                   containerFactory = "kafkaEventDispatcherContainerFactory",
                   groupId = "#{'${spring.application.name}-' + T(java.lang.management.ManagementFactory).getRuntimeMXBean().getName() + '-' + T(java.util.UUID).randomUUID()}")
    public void dispatch(Event event, @Header(KafkaHeaders.GROUP_ID) String groupId) {
        try {
            super.dispatch(event);
            log.debug("Event received by ReplicasEventDispatcher: {}, groupId: {}", event.asJson(), groupId);
        } catch (Exception e) {
            log.error("Error processing event from topic {}: {}", event, e.getMessage(), e);
        }
    }
}
