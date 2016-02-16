package com.oasisdigital.nges.event;

/**
 *
 * Thrown when appending to the event store when:
 * <ul>
 * <li>the stream already exists and lastSequence is 0 or {@link #NEW_STREAM}</li>
 * <li>the stream has been appended concurrently and its current lastSequence does not match the one provided
 * on this call</li>
 * <li>the stream already exists, but has different streamType</li>
 * </ul>
 *
 */
public class EventStoreConflict extends EventStoreException {
    private static final long serialVersionUID = 1L;

    public EventStoreConflict() {
        super();
    }

    public EventStoreConflict(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public EventStoreConflict(String message, Throwable cause) {
        super(message, cause);
    }

    public EventStoreConflict(String message) {
        super(message);
    }

    public EventStoreConflict(Throwable cause) {
        super(cause);
    }

}
