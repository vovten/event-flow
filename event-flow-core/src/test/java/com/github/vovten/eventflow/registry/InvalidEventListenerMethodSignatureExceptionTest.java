package com.github.vovten.eventflow.registry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for InvalidEventListenerMethodSignatureException.
 */
@DisplayName("InvalidEventListenerMethodSignatureException Tests")
class InvalidEventListenerMethodSignatureExceptionTest {

    @Test
    @DisplayName("Should create exception with class name and method name")
    void shouldCreateExceptionWithClassNameAndMethodName() {
        String className = "com.example.MyListener";
        String methodName = "handleEvent";
        InvalidEventListenerMethodSignatureException exception =
                new InvalidEventListenerMethodSignatureException(className, methodName);

        assertTrue(exception.getMessage().contains(className));
        assertTrue(exception.getMessage().contains(methodName));
        assertNull(exception.getCause());
    }

    @Test
    @DisplayName("Should be RuntimeException")
    void shouldBeRuntimeException() {
        InvalidEventListenerMethodSignatureException exception =
                new InvalidEventListenerMethodSignatureException("MyListener", "handleEvent");

        assertTrue(exception instanceof RuntimeException);
    }
}
