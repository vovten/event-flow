package io.github.vovten.eventflow.autoconfig.config;

import io.github.vovten.eventflow.autoconfig.EventFlowProperties;
import io.github.vovten.eventflow.autoconfig.transport.InTransportFactory;
import io.github.vovten.eventflow.autoconfig.transport.OutTransportFactory;
import io.github.vovten.eventflow.transport.InTransport;
import io.github.vovten.eventflow.transport.OutTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for duplicate transport factory registration.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("Duplicate Transport Factory Tests")
class DuplicateTransportFactoryTest {

    @Test
    @DisplayName("DEFECT-7.4: Duplicate publisher factories fail fast in ChannelConfiguration")
    void shouldFailFastForDuplicatePublisherFactoriesInChannelConfiguration() {
        assertThatThrownBy(() -> new ChannelConfiguration(new EventFlowProperties(),
                List.of(outFactory("dup"), outFactory("dup"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate publisher transport factory")
                .hasMessageContaining("dup");
    }

    @Test
    @DisplayName("DEFECT-7.4: Duplicate publisher factories fail fast in PublisherConfiguration")
    void shouldFailFastForDuplicatePublisherFactoriesInPublisherConfiguration() {
        assertThatThrownBy(() -> new PublisherConfiguration(new EventFlowProperties(),
                List.of(outFactory("dup"), outFactory("dup"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate publisher transport factory");
    }

    @Test
    @DisplayName("DEFECT-7.4: Duplicate dispatcher factories fail fast in DispatcherConfiguration")
    void shouldFailFastForDuplicateDispatcherFactories() {
        assertThatThrownBy(() -> new DispatcherConfiguration(new EventFlowProperties(),
                List.of(inFactory("dup"), inFactory("dup"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate dispatcher transport factory")
                .hasMessageContaining("dup");
    }

    private static OutTransportFactory outFactory(String name) {
        return new OutTransportFactory() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public OutTransport createPublisher(EventFlowProperties.TransportConfig config) {
                return null;
            }
        };
    }

    private static InTransportFactory inFactory(String type) {
        return new InTransportFactory() {
            @Override
            public String getType() {
                return type;
            }

            @Override
            public InTransport createDispatcher(EventFlowProperties.TransportConfig config) {
                return null;
            }
        };
    }
}
