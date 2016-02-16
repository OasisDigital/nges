package com.oasisdigital.nges.event;

/**
 * Base exception class. May indicate an infrastructure problem (database unavailable or corrupted), as well
 * as any other erroneous condition such as concurrent modification or constraint violation.
 *
 */
public class EventStoreException extends RuntimeException {

    private static final long serialVersionUID = -1237680703226042282L;

    public EventStoreException() {
        super();
    }

    public EventStoreException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public EventStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public EventStoreException(String message) {
        super(message);
    }

    public EventStoreException(Throwable cause) {
        super(cause);
    }
}
