package io.github.vovten.eventflow.autoconfig;

import io.github.vovten.eventflow.autoconfig.config.*;
import io.github.vovten.eventflow.publisher.EventPublisher;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

/**
 * Main auto-configuration class for Event Flow components in Spring applications.
 * <p>
 * This is a facade that imports modular configuration classes:
 * <ul>
 *   <li>{@link SerializerConfiguration} - custom event serializers (registered in constructor for early initialization)</li>
 *   <li>{@link RegistryConfiguration} - event listener registries</li>
 *   <li>{@link CommonConfiguration} - executor service and local-queue transports</li>
 *   <li>{@link ChannelConfiguration} - event channels</li>
 *   <li>{@link PublisherConfiguration} - event publisher</li>
 *   <li>{@link DispatcherConfiguration} - event dispatcher</li>
 * </ul>
 * <p>
 * <b>Configuration options:</b>
 * <ul>
 *   <li>Set {@code event-flow.enabled=true} to enable all auto-configuration (disabled by default)</li>
 *   <li>Set {@code event-flow.publisher.enabled=true} to enable publisher (disabled by default)</li>
 *   <li>Set {@code event-flow.dispatcher.enabled=true} to enable dispatcher (disabled by default)</li>
 *   <li>Set {@code event-flow.dispatcher.idempotent.enabled=true} to enable idempotent event processing</li>
 * </ul>
 * <p>
 * <b>Usage example (application.yml):</b>
 * <pre>{@code
 * event-flow:
 *   enabled: true
 *   scan-packages: com.example.listener
 *   publisher:
 *     enabled: true
 *     channels:
 *       - name: internal
 *         transports:
 *           - name: local-queue
 *             capacity: 1000
 *   dispatcher:
 *     enabled: true
 *     idempotent:
 *       enabled: true
 *       ttl: 10m
 *       max-size: 10000
 *     transports:
 *       - name: local-queue
 *         capacity: 1000
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@AutoConfiguration
@ConditionalOnClass(EventPublisher.class)
@EnableConfigurationProperties(EventFlowProperties.class)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true")
@Import({
    SerializerConfiguration.class,
    RegistryConfiguration.class,
    CommonConfiguration.class,
    ChannelConfiguration.class,
    PublisherConfiguration.class,
    DispatcherConfiguration.class
})
public class EventFlowAutoConfiguration {
}
