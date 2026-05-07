package io.github.vovten.eventflow.transport.outgoing.rabbitmq;

import com.rabbitmq.client.*;
import io.github.vovten.eventflow.event.Event;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RabbitMqOutTransport constructors and name.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RabbitMqOutTransport Tests")
class RabbitMqOutTransportTest {

    @Mock
    private Connection connection;

    @Mock
    private Channel channel;

    @Test
    @DisplayName("Should have rabbitmq as transport name")
    void shouldHaveRabbitmqName() throws Exception {
        when(connection.createChannel()).thenReturn(channel);
        RabbitMqOutTransport transport = new RabbitMqOutTransport(connection, "test-exchange", "test-key");
        assertEquals("rabbitmq", transport.name());
    }

    private static class TestEvent implements Event {
        @Override
        public Class<? extends Event> type() {
            return TestEvent.class;
        }
    }
}
