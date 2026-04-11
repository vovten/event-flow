package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.serialization.EventSerializer;
import io.github.vovten.eventflow.serialization.EventSerializerFactory;
import io.github.vovten.eventflow.serialization.EventTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * Auto-configuration for custom event serializers and type security.
 * <p>
 * Creates an {@link EventSerializerFactory} bean and automatically discovers all
 * {@link EventSerializer} beans in the Spring context, registering them in the factory.
 * <p>
 * Also registers allowed event packages in {@link EventTypeRegistry} for secure
 * deserialization.
 * <p>
 * <b>Usage:</b> Create a class implementing {@link EventSerializer}, annotate it
 * with {@code @Component}, and it will be automatically registered:
 * <pre>{@code
 * @Component
 * public class ProtobufEventSerializer implements EventSerializer {
 *     @Override
 *     public byte getCode() { return 0x03; }
 *
 *     @Override
 *     public String getName() { return "protobuf"; }
 *
 *     // ... serialize/deserialize implementations
 * }
 * }</pre>
 * <p>
 * This configuration is imported by {@link io.github.vovten.eventflow.autoconfig.EventFlowAutoConfiguration}
 * and is activated when {@code event-flow.enabled=true}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-04-03
 * @see EventSerializer
 * @see EventSerializerFactory
 * @see EventTypeRegistry
 */
@Configuration(proxyBeanMethods = false)
public class SerializerConfiguration {

    private static final Logger log = LoggerFactory.getLogger(SerializerConfiguration.class);

    private final Map<String, EventSerializer> serializers;
    private final EventFlowProperties properties;

    /**
     * Constructs SerializerConfiguration.
     *
     * @param serializers map of EventSerializer beans discovered by Spring
     * @param properties event flow configuration properties
     */
    public SerializerConfiguration(Map<String, EventSerializer> serializers, EventFlowProperties properties) {
        this.serializers = serializers;
        this.properties = properties;
    }

    /**
     * Creates and configures EventSerializerFactory bean.
     * <p>
     * All discovered EventSerializer beans are registered in the factory.
     *
     * @return configured EventSerializerFactory
     */
    @Bean
    public EventSerializerFactory eventSerializerFactory() {
        EventSerializerFactory factory = new EventSerializerFactory();
        registerSerializers(factory);
        return factory;
    }

    /**
     * Registers allowed event packages in EventTypeRegistry for secure deserialization
     * and returns a marker bean for @DependsOn references.
     * <p>
     * Packages are taken from {@code event-flow.dispatcher.deserialization.allowed-event-packages}.
     * If not explicitly configured, defaults to {@code io.github.vovten.eventflow}.
     *
     * @return marker object indicating serializer registration is complete
     */
    @Bean(name = "serializerRegistrationComplete")
    public Object serializerRegistrationMarker() {
        registerAllowedEventPackages();
        return new Object();
    }

    /**
     * Registers all discovered EventSerializer beans into EventSerializerFactory.
     * <p>
     * Each serializer is registered by name and code (both indexes populated automatically).
     * If a serializer with the same name or code already exists, it will be overridden.
     *
     * @param factory the serializer factory to register serializers in
     */
    private void registerSerializers(EventSerializerFactory factory) {
        if (serializers.isEmpty()) {
            log.debug("No custom EventSerializer beans found");
            return;
        }
        log.info("Registering {} custom EventSerializer bean(s)", serializers.size());
        for (Map.Entry<String, EventSerializer> entry : serializers.entrySet()) {
            String beanName = entry.getKey();
            EventSerializer serializer = entry.getValue();

            factory.register(serializer);
            log.info("Registered serializer '{}' [code=0x{}, name={}]",
                    beanName, Integer.toHexString(serializer.getCode() & 0xFF), serializer.getName());
        }
    }

    /**
     * Registers allowed event packages in EventTypeRegistry for secure deserialization.
     * <p>
     * Packages are taken from {@code event-flow.dispatcher.deserialization.allowed-event-packages}.
     * If not explicitly configured, defaults to {@code io.github.vovten.eventflow}.
     */
    private void registerAllowedEventPackages() {
        var allowedPackages = properties.getDispatcher()
                .getDeserialization()
                .getAllowedEventPackages();

        if (allowedPackages == null || allowedPackages.isEmpty()) {
            log.warn("No allowed event packages configured. Using default: io.github.vovten.eventflow");
            EventTypeRegistry.allowPackage("io.github.vovten.eventflow");
            return;
        }

        log.info("Registering {} allowed event package(s) for secure deserialization", allowedPackages.size());
        for (String packageName : allowedPackages) {
            EventTypeRegistry.allowPackage(packageName);
            log.info("Registered allowed event package: {}", packageName);
        }
    }
}
