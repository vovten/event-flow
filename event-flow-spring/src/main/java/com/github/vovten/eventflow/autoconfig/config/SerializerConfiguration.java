package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.serialization.EventSerializer;
import com.github.vovten.eventflow.serialization.EventSerializerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * Auto-configuration for custom event serializers.
 * <p>
 * Automatically discovers all {@link EventSerializer} beans in the Spring context
 * and registers them in {@link EventSerializerFactory} by name and code.
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
 * This configuration is imported by {@link com.github.vovten.eventflow.autoconfig.EventFlowAutoConfiguration}
 * and is activated when {@code event-flow.enabled=true}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-04-03
 * @see EventSerializer
 * @see EventSerializerFactory
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
public class SerializerConfiguration {

    private final Map<String, EventSerializer> serializers;

    /**
     * Constructs SerializerConfiguration and immediately registers all discovered
     * EventSerializer beans into EventSerializerFactory.
     * <p>
     * Registration happens in constructor to ensure serializers are available
     * before any transport factory creates transports that depend on them.
     *
     * @param serializers map of EventSerializer beans discovered by Spring
     */
    public SerializerConfiguration(Map<String, EventSerializer> serializers) {
        this.serializers = serializers;
        registerSerializers();
    }

    /**
     * Returns a marker bean to allow @DependsOn references.
     * This ensures other configurations wait for serializer registration completion.
     *
     * @return marker object indicating serializers are registered
     */
    @Bean(name = "serializerRegistrationComplete")
    public Object serializerRegistrationMarker() {
        return new Object();
    }

    /**
     * Registers all discovered EventSerializer beans into EventSerializerFactory.
     * <p>
     * Each serializer is registered by name and code (both indexes populated automatically).
     * If a serializer with the same name or code already exists, it will be overridden.
     */
    private void registerSerializers() {
        if (serializers.isEmpty()) {
            log.debug("No custom EventSerializer beans found");
            return;
        }
        log.info("Registering {} custom EventSerializer bean(s)", serializers.size());
        for (Map.Entry<String, EventSerializer> entry : serializers.entrySet()) {
            String beanName = entry.getKey();
            EventSerializer serializer = entry.getValue();

            EventSerializerFactory.register(serializer);
            log.info("Registered serializer '{}' [code=0x{}, name={}]",
                    beanName, Integer.toHexString(serializer.getCode() & 0xFF), serializer.getName());
        }
    }
}
