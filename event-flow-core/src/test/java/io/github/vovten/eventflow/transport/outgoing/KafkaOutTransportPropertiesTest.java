package io.github.vovten.eventflow.transport.outgoing;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the producer defaults applied by {@link AbstractKafkaOutTransport}.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("Kafka Producer Default Properties Tests")
class KafkaOutTransportPropertiesTest {

    @Test
    @DisplayName("DEFECT-6.4: Default max.request.size matches the typical broker message limit")
    void shouldDefaultMaxRequestSizeToBrokerLimit() {
        Properties props = AbstractKafkaOutTransport.buildProducerProperties(new Properties());

        assertThat(props.getProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG))
                .as("A 5MB producer limit only moves the failure to a 1MB broker")
                .isEqualTo("1048576");
    }

    @Test
    @DisplayName("DEFECT-6.4: An explicit max.request.size wins over the default")
    void shouldRespectExplicitMaxRequestSize() {
        Properties overrides = new Properties();
        overrides.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "5242880");

        Properties props = AbstractKafkaOutTransport.buildProducerProperties(overrides);

        assertThat(props.getProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG)).isEqualTo("5242880");
    }

    @Test
    @DisplayName("Should keep the at-least-once producer defaults")
    void shouldKeepAtLeastOnceProducerDefaults() {
        Properties props = AbstractKafkaOutTransport.buildProducerProperties(new Properties());

        assertThat(props.getProperty(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
        assertThat(props.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isEqualTo("true");
        assertThat(props.getProperty(ProducerConfig.RETRIES_CONFIG)).isEqualTo("3");
    }
}
