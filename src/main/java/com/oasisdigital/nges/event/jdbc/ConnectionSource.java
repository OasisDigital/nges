package com.oasisdigital.nges.event.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.oasisdigital.nges.event.EventStoreException;

class ConnectionSource {
    private final ThreadLocal<Connection> connection;
    private final DataSource dataSource;

    public ConnectionSource(DataSource dataSource) {
        this.connection = new ThreadLocal<>();
        this.dataSource = dataSource;
    }

    public <T> T withConnection(Callback<T> callback) throws EventStoreException {
        Connection threadConn = connection.get();
        if (threadConn != null) {
            try {
                return callback.execute(threadConn);
            } catch (SQLException e) {
                throw SQLExceptionTranslation.translate(e);
            }
        } else {
            try (Connection conn = dataSource.getConnection()) {
                connection.set(conn);
                return callback.execute(conn);
            } catch (SQLException e) {
                throw SQLExceptionTranslation.translate(e);
            } finally {
                connection.remove();
            }
        }
    }

    public <T> T inTransaction(Callback<T> callback) throws EventStoreException {
        return withConnection(conn -> {
            boolean autoCommit = conn.getAutoCommit();
            try {
                conn.setAutoCommit(false);
                T result = callback.execute(conn);
                conn.commit();
                return result;
            } finally {
                conn.rollback();
                conn.setAutoCommit(autoCommit);
            }
        });
    }

    @FunctionalInterface
    public static interface Callback<ReturnType> {
        public ReturnType execute(Connection connection) throws SQLException, EventStoreException;
    }
}
