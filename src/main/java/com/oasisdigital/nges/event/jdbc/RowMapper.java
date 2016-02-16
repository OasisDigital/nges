package com.oasisdigital.nges.event.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
interface RowMapper<T> {
    public T toEvent(ResultSet rs) throws SQLException;
}
