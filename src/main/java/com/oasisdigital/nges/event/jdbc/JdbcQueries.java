package com.oasisdigital.nges.event.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.oasisdigital.nges.event.EventStoreException;

class JdbcQueries {
    private final ConnectionSource connectionSource;

    public JdbcQueries(ConnectionSource connectionSource) {
        super();
        this.connectionSource = connectionSource;
    }

    public <T> T queryForObject(String query, RowMapper<T> mapper, Object... params)
            throws EventStoreException {
        return queryForOptionalObject(query, mapper, params)
                .orElseThrow(() -> new EventStoreException("No matching rows for query: [" + query + "]"));
    }

    @SuppressWarnings("unchecked")
    public <T> T queryForObject(String query, Class<T> type, Object... params) throws EventStoreException {
        return queryForObject(query, rs -> (T) rs.getObject(1), params);
    }

    public <T> Optional<T> queryForOptionalObject(String query, RowMapper<T> mapper, Object... params)
            throws EventStoreException {
        ResultSetMapper<Optional<T>> rsMapper = rs -> {
            if (rs.next()) {
                T result = mapper.toEvent(rs);
                if (rs.next()) {
                    throw new EventStoreException("More than one matching row for query: [" + query + "]");
                } else {
                    return Optional.ofNullable(result);
                }
            } else {
                return Optional.empty();
            }
        };
        return query(query, rsMapper, params);
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<T> queryForOptionalObject(String query, Class<T> type, Object... params)
            throws EventStoreException {
        return queryForOptionalObject(query, rs -> (T) rs.getObject(1), params);
    }

    public <T> List<T> queryForList(String query, RowMapper<T> mapper, Object... params)
            throws EventStoreException {
        return query(query, rs -> {
            List<T> result = new ArrayList<>();
            while (rs.next()) {
                result.add(mapper.toEvent(rs));
            }
            return result;
        } , params);
    }

    public <T> T query(String query, ResultSetMapper<T> mapper, Object... params) throws EventStoreException {
        return connectionSource.withConnection(conn -> {
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                setParams(ps, params);
                return mapper.mapResultSet(ps.executeQuery());
            }
        });
    }

    public int update(String query, Object... params) throws EventStoreException {
        return connectionSource.withConnection(conn -> {
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                setParams(ps, params);
                return ps.executeUpdate();
            }
        });
    }

    public int insert(String query, Object... params) throws EventStoreException {
        return update(query, params);
    }

    public int insert(String query, List<Map<String, Object>> keyHolder, Object... params)
            throws EventStoreException {
        return connectionSource.withConnection(conn -> {
            try (PreparedStatement ps = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
                setParams(ps, params);
                int result = ps.executeUpdate();
                ResultSet keys = ps.getGeneratedKeys();
                while (keys.next()) {
                    ResultSetMetaData meta = keys.getMetaData();
                    Map<String, Object> rowKeys = new HashMap<>();
                    for (int i = 0; i < meta.getColumnCount(); i++) {
                        rowKeys.put(meta.getColumnName(i + 1), keys.getObject(i + 1));
                    }
                    keyHolder.add(rowKeys);
                }
                return result;
            }
        });
    }

    private void setParams(PreparedStatement ps, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            ps.setObject(i + 1, params[i]);
        }
    }

}
