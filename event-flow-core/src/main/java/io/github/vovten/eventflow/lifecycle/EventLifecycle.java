package io.github.vovten.eventflow.lifecycle;

/**
 * Defines the level of lifecycle tracking for an event.
 * <p>
 * Applied via the {@link io.github.vovten.eventflow.event.annotation.Event @Event} annotation.
 * Controls whether an event is persisted and whether acknowledgment events
 * ({@code SuccessAck} / {@code FailureAck}) are generated.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public enum EventLifecycle {

    /**
     * Fire-and-forget. The event is not persisted and no lifecycle tracking is performed.
     */
    NONE,

    /**
     * The event is persisted with status {@link io.github.vovten.eventflow.lifecycle.store.EventStatus#UNDEFINED}
     * but no lifecycle tracking is performed. The event record is stored for reference
     * without any status transitions or retry capability.
     */
    PERSISTED,

    /**
     * Full lifecycle tracking. The event is persisted with status
     * {@link io.github.vovten.eventflow.lifecycle.store.EventStatus#NEW} and publication status is tracked
     * ({@code NEW → PUBLISHED / FAILED}). When handled by a dispatcher, acknowledgment events
     * ({@code SuccessAck} / {@code FailureAck}) are generated,
     * enabling end-to-end tracking ({@code NEW → PUBLISHED → HANDLED / FAILED}).
     */
    MANAGED
}
