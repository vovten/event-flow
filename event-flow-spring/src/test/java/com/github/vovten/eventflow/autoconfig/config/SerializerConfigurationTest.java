package com.github.vovten.eventflow.autoconfig.config;

import com.github.vovten.eventflow.autoconfig.EventFlowAutoConfiguration;
import com.github.vovten.eventflow.autoconfig.EventFlowProperties;
import com.github.vovten.eventflow.autoconfig.transport.outgoing.KafkaOutTransportFactory;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.serialization.EventSerializer;
import com.github.vovten.eventflow.serialization.EventSerializerFactory;
import com.github.vovten.eventflow.transport.OutTransport;
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
        assertThat(customSerializer.getFormatCode()).isEqualTo((byte) 0x03);
        assertThat(customSerializer.getFormat()).isEqualTo("custom-protobuf");
    }

    @Test
    @DisplayName("Should register custom serializer in EventSerializerFactory via @PostConstruct")
    void shouldRegisterSerializerInFactory() {
        // given
        assertThat(context).isNotNull();

        // when - check that custom serializer is registered
        EventSerializer serializer = EventSerializerFactory.getByFormatCode((byte) 0x03);

        // then
        assertThat(serializer).isNotNull();
        assertThat(serializer.getFormat()).isEqualTo("custom-protobuf");
        assertThat(serializer).isInstanceOf(CustomEventSerializer.class);
    }

    @Test
    @DisplayName("Should make custom serializer available to KafkaOutTransportFactory")
    void shouldMakeSerializerAvailableToKafkaFactory() {
        // given
        KafkaOutTransportFactory factory = context.getBean(KafkaOutTransportFactory.class);
        assertThat(factory).isNotNull();

        // when - create transport with custom format
        EventFlowProperties.TransportConfig config = new EventFlowProperties.TransportConfig();
        config.setName("kafka");
        config.setServers("localhost:9092");
        config.setTopic("test-topic");
        config.setSerialization("custom-protobuf");

        OutTransport transport = factory.createPublisher(config);

        // then - transport should be created successfully with custom serializer
        assertThat(transport).isNotNull();
    }

    @Test
    @DisplayName("Should have custom serializer registered before transport factories are initialized")
    void shouldRegisterBeforeTransportFactories() {
        // given
        assertThat(context).isNotNull();

        // when - get all registered format codes
        var formatCodes = EventSerializerFactory.getRegisteredFormatCodes();

        // then - custom format code 0x03 should be registered
        assertThat(formatCodes).contains((byte) 0x01);  // JSON
        assertThat(formatCodes).contains((byte) 0x02);  // MessagePack
        assertThat(formatCodes).contains((byte) 0x03);  // Custom protobuf
    }

    @Test
    @DisplayName("Should create channels with custom serializer in configuration")
    void shouldCreateChannelsWithCustomSerializer() {
        // given - custom serializer is registered via @PostConstruct
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
        public byte getFormatCode() {
            return 0x03;
        }

        @Override
        public String getFormat() {
            return "custom-protobuf";
        }
    }
}
