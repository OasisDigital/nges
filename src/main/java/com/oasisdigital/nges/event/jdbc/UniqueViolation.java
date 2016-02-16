package com.oasisdigital.nges.event.jdbc;

import com.oasisdigital.nges.event.EventStoreException;

public class UniqueViolation extends EventStoreException {

    private static final long serialVersionUID = 8965563560973401761L;

    public UniqueViolation() {
        super();
    }

    public UniqueViolation(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public UniqueViolation(String message, Throwable cause) {
        super(message, cause);
    }

    public UniqueViolation(String message) {
        super(message);
    }

    public UniqueViolation(Throwable cause) {
        super(cause);
    }

}
