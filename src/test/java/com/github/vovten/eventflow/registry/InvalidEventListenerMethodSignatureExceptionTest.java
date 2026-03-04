package com.github.vovten.eventflow.registry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for InvalidEventListenerMethodSignatureException
 */
class InvalidEventListenerMethodSignatureExceptionTest {

    @Test
    @DisplayName("Should create exception with class and method name")
    void shouldCreateExceptionWithClassAndMethodName() {
        // given
        String className = "com.example.TestListener";
        String methodName = "handleEvent";

        // when
        InvalidEventListenerMethodSignatureException exception =
            new InvalidEventListenerMethodSignatureException(className, methodName);

        // then
        assertNotNull(exception.getMessage());
        assertTrue(exception.getMessage().contains(className));
        assertTrue(exception.getMessage().contains(methodName));
        assertTrue(exception.getMessage().contains("Method signature"));
    }

    @Test
    @DisplayName("Should format message correctly")
    void shouldFormatMessageCorrectly() {
        // given
        String className = "com.github.vovten.eventflow.MyListener";
        String methodName = "onTestEvent";

        // when
        InvalidEventListenerMethodSignatureException exception =
            new InvalidEventListenerMethodSignatureException(className, methodName);

        // then
        String expectedMessage = String.format(
            "Method signature does not meet the EventListener annotation requirements. " +
            "Class: %s, method: %s", className, methodName);
        assertEquals(expectedMessage, exception.getMessage());
    }
}
