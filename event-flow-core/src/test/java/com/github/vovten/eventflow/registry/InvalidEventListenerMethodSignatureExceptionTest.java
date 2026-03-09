package com.github.vovten.eventflow.registry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link InvalidEventListenerMethodSignatureException}.
 *
 * @author Vladimir Aleshkov
 * @since 2026-03-09
 */
@DisplayName("InvalidEventListenerMethodSignatureException Tests")
class InvalidEventListenerMethodSignatureExceptionTest {

    @Test
    @DisplayName("Should create exception with listener class and method name")
    void shouldCreateExceptionWithListenerClassAndMethodName() {
        // Act
        InvalidEventListenerMethodSignatureException exception = 
                new InvalidEventListenerMethodSignatureException("com.example.Listener", "handleEvent");

        // Assert
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("com.example.Listener"));
        assertTrue(exception.getMessage().contains("handleEvent"));
    }
}
