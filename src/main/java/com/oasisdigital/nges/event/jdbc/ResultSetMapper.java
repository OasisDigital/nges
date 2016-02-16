package com.oasisdigital.nges.event.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.oasisdigital.nges.event.EventStoreException;

@FunctionalInterface
interface ResultSetMapper<T> {
    public T mapResultSet(ResultSet rs) throws SQLException, EventStoreException;
}
