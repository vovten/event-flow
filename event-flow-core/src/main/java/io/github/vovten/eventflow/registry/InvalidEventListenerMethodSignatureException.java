package io.github.vovten.eventflow.registry;

/**
 * Exception thrown when the signature of a method responsible for handling
 * an event does not meet the requirements
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
public class InvalidEventListenerMethodSignatureException extends RuntimeException {

    public InvalidEventListenerMethodSignatureException(String className, String methodName) {
        super(String.format("Method signature does not meet the EventListener annotation requirements. " +
                "Class: %s, method: %s", className, methodName));
    }
}
