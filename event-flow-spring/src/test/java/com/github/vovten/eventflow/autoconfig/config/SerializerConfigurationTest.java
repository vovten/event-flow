package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowAutoConfiguration;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.serialization.EventSerializer;
import com.github.vovten.eventflow.serialization.EventSerializerFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link SerializerConfiguration}.
 * Verifies that custom event serializers are properly registered and available
 * before transport factories create transports that depend on them.
 */
@SpringBootTest(
        classes = {
            EventFlowAutoConfiguration.class,
            SerializerConfigurationTest.CustomSerializerTestConfig.class
        }
)
@TestPropertySource(properties = {
    "event-flow.enabled=true",
    "event-flow.scan-packages=com.github.vovten.eventflow",
    "event-flow.publisher.enabled=true",
    "event-flow.publisher.channels[0].name=test-channel",
    "event-flow.publisher.channels[0].transports[0].name=kafka",
    "event-flow.publisher.channels[0].transports[0].servers=localhost:9092",
    "event-flow.publisher.channels[0].transports[0].topic=test-topic",
    "event-flow.publisher.channels[0].transports[0].serialization=custom-protobuf"
})
class SerializerConfigurationTest {

    @Autowired
    private ApplicationContext context;

    @Test
    @DisplayName("Should register custom EventSerializer beans during configuration")
    void shouldRegisterCustomSerializerBeans() {
        // given
        assertThat(context).isNotNull();

        // when
        CustomEventSerializer customSerializer = context.getBean(CustomEventSerializer.class);

        // then
        assertThat(customSerializer).isNotNull();
        assertThat(customSerializer.getCode()).isEqualTo((byte) 0x03);
        assertThat(customSerializer.getName()).isEqualTo("custom-protobuf");
    }

    @Test
    @DisplayName("Should register custom serializer in EventSerializerFactory via constructor")
    void shouldRegisterSerializerInFactory() {
        // given
        assertThat(context).isNotNull();

        // when - check that custom serializer is registered
        EventSerializer serializer = EventSerializerFactory.getByCode((byte) 0x03);

        // then
        assertThat(serializer).isNotNull();
        assertThat(serializer.getName()).isEqualTo("custom-protobuf");
        assertThat(serializer).isInstanceOf(CustomEventSerializer.class);
    }

    @Test
    @DisplayName("Should make custom serializer available to KafkaOutTransportFactory by name")
    void shouldMakeSerializerAvailableToKafkaFactoryByName() {
        // given
        EventSerializer serializer = EventSerializerFactory.getByName("custom-protobuf");

        // then
        assertThat(serializer).isNotNull();
        assertThat(serializer.getName()).isEqualTo("custom-protobuf");
        assertThat(serializer.getCode()).isEqualTo((byte) 0x03);
    }

    @Test
    @DisplayName("Should have custom serializer registered before transport factories are initialized")
    void shouldRegisterBeforeTransportFactories() {
        // given
        assertThat(context).isNotNull();

        // when - get all registered serializer names and codes
        var names = EventSerializerFactory.getRegisteredNames();
        var codes = EventSerializerFactory.getRegisteredCodes();

        // then - custom serializer should be registered
        assertThat(names).contains("json", "msgpack", "custom-protobuf");
        assertThat(codes).contains((byte) 0x01, (byte) 0x02, (byte) 0x03);
    }

    @Test
    @DisplayName("Should create channels with custom serializer in configuration")
    void shouldCreateChannelsWithCustomSerializer() {
        // given - custom serializer is registered via constructor
        // when - event channels are created with custom serialization format
        var channels = context.getBean("eventChannels", java.util.List.class);

        // then - channels should be created successfully with custom serializer
        assertThat(channels).isNotNull();
        assertThat(channels).isNotEmpty();
    }

    /**
     * Test configuration that provides a custom EventSerializer bean.
     */
    @Configuration
    static class CustomSerializerTestConfig {
        @Bean
        public CustomEventSerializer customEventSerializer() {
            return new CustomEventSerializer();
        }
    }

    /**
     * Custom serializer implementation for testing purposes.
     */
    @Component
    static class CustomEventSerializer implements EventSerializer {
        @Override
        public byte[] serialize(Event event) {
            return new byte[]{0x03};
        }

        @Override
        public <T extends Event> T deserialize(byte[] data, Class<T> eventType) {
            try {
                return eventType.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize", e);
            }
        }

        @Override
        public byte getCode() {
            return 0x03;
        }

        @Override
        public String getName() {
            return "custom-protobuf";
        }
    }
}
