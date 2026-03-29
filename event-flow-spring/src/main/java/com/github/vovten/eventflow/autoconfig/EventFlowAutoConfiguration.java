package com.github.vovten.eventflow.autoconfig;

import com.github.vovten.eventflow.autoconfig.config.*;
import com.github.vovten.eventflow.publisher.EventPublisher;
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
 *   <li>{@link RegistryConfiguration} - event listener registries</li>
 *   <li>{@link CommonConfiguration} - executor service and local-queue transports</li>
 *   <li>{@link ChannelConfiguration} - event channels</li>
 *   <li>{@link PublisherConfiguration} - event publisher</li>
 *   <li>{@link DispatcherConfiguration} - event dispatcher</li>
 * </ul>
 * <p>
 * <b>Configuration options:</b>
 * <ul>
 *   <li>Set {@code event-flow.enabled=false} to disable all auto-configuration</li>
 *   <li>Set {@code event-flow.publisher.enabled=false} to disable publisher only</li>
 *   <li>Set {@code event-flow.dispatcher.enabled=false} to disable dispatcher only</li>
 * </ul>
 * <p>
 * <b>Usage example (application.yml):</b>
 * <pre>{@code
 * event-flow:
 *   scan-packages: com.example.listener
 *   publisher:
 *     transactional: true
 *     retry:
 *       enabled: true
 *       max-attempts: 3
 *   dispatcher:
 *     thread-pool:
 *       core-size: 4
 *       max-size: 16
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@AutoConfiguration
@ConditionalOnClass(EventPublisher.class)
@EnableConfigurationProperties(EventFlowProperties.class)
@ConditionalOnProperty(prefix = "event-flow", name = "enabled", havingValue = "true", matchIfMissing = true)
@Import({
    RegistryConfiguration.class,
    CommonConfiguration.class,
    ChannelConfiguration.class,
    PublisherConfiguration.class,
    DispatcherConfiguration.class
})
public class EventFlowAutoConfiguration {
}
