package com.oasisdigital.nges.event.jdbc;

import java.sql.SQLException;

import com.oasisdigital.nges.event.EventStoreException;

class SQLExceptionTranslation {
    // http://www.postgresql.org/docs/9.3/static/errcodes-appendix.html
    private static final String UNIQUE_VIOLATION = "23505";

    public static EventStoreException translate(SQLException e) {
        if (UNIQUE_VIOLATION.equals(e.getSQLState())) {
            return new UniqueViolation(e);
        }
        return new EventStoreException("Unable to execute query", e);
    }

}
