package com.github.vovten.eventflow.serialization;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for EventTypeRegistry security validation
 */
@DisplayName("EventTypeRegistry Security Tests")
class EventTypeRegistryTest {

    @AfterEach
    void tearDown() {
        EventTypeRegistry.clear();
    }

    @Test
    @DisplayName("Should allow event-flow package classes by default")
    void defaultAllowsEventFlowPackage() {
        // Classes from event-flow package should be allowed by default
        assertTrue(EventTypeRegistry.isAllowed("com.github.vovten.eventflow.event.Event"));
        assertTrue(EventTypeRegistry.isAllowed("com.github.vovten.eventflow.test.TestEvent"));
    }

    @Test
    @DisplayName("Should block external packages by default")
    void blocksExternalPackagesByDefault() {
        // Classes from external packages should be blocked
        assertFalse(EventTypeRegistry.isAllowed("com.example.malicious.HackEvent"));
        assertFalse(EventTypeRegistry.isAllowed("java.util.HashMap"));
        assertFalse(EventTypeRegistry.isAllowed("org.apache.commons.collections.Factory"));
    }

    @Test
    @DisplayName("Should allow package when explicitly registered")
    void allowPackageAddsAccess() {
        EventTypeRegistry.allowPackage("com.example.events");

        assertTrue(EventTypeRegistry.isAllowed("com.example.events.OrderCreated"));
        assertTrue(EventTypeRegistry.isAllowed("com.example.events.user.UserRegistered"));
        assertFalse(EventTypeRegistry.isAllowed("com.other.events.Event"));
    }

    @Test
    @DisplayName("Should allow class when explicitly registered")
    void allowClassAddsSpecificAccess() {
        // External class not allowed by default
        assertFalse(EventTypeRegistry.isAllowed("com.external.UnauthorizedEvent"));

        // But package-based allowance works
        EventTypeRegistry.allowPackage("com.external");
        assertTrue(EventTypeRegistry.isAllowed("com.external.UnauthorizedEvent"));
    }

    @Test
    @DisplayName("Should reject null and empty class names")
    void validationRejectsNullAndEmpty() {
        assertFalse(EventTypeRegistry.isAllowed(null));
        assertFalse(EventTypeRegistry.isAllowed(""));
    }

    @Test
    @DisplayName("Should reject invalid package names")
    void allowPackageRejectsInvalidInput() {
        assertThrows(IllegalArgumentException.class, () -> EventTypeRegistry.allowPackage(null));
        assertThrows(IllegalArgumentException.class, () -> EventTypeRegistry.allowPackage(""));
    }

    @Test
    @DisplayName("Should reject null class registration")
    void allowClassRejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> EventTypeRegistry.allowClass(null));
    }
}
