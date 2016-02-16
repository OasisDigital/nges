package com.oasisdigital.nges.event.jdbc;

import static java.util.stream.Collectors.toList;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.oasisdigital.nges.event.Event;
import com.oasisdigital.nges.event.EventStoreException;

class EventLogDao {
    private final JdbcQueries queries;

    public EventLogDao(ConnectionSource connectionSource) {
        this.queries = new JdbcQueries(connectionSource);
    }

    public Event getEvent(long eventId) throws EventStoreException {
        return queries.queryForObject("select * from event_log where event_id = ?", EventLogDao::toEvent,
                eventId);
    }

    public List<Event> getLatestEvents(int limit) throws EventStoreException {
        return queryForList("select * from event_log order by event_id desc limit ?", limit);
    }

    public List<Event> getEventsForAllStreams(long afterEventId, int limit) throws EventStoreException {
        return queryForList("select * from event_log where event_id > ? order by event_id limit ?",
                afterEventId, limit);
    }

    public List<Event> getEventsForStream(UUID streamId, long afterSequenceId, int limit)
            throws EventStoreException {
        return queryForList(
                "select * from event_log where stream_id = ? and seq_no > ? order by seq_no limit ?",
                streamId, afterSequenceId, limit);
    }

    public List<Long> save(List<Event> events) throws EventStoreException {
        if (events.isEmpty()) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> keyHolder = new LinkedList<>();
        List<Object> values = new ArrayList<>();
        StringBuilder statement = new StringBuilder();
        statement.append("insert into event_log(stream_id, type, correlation_id, seq_no, payload) values\n");
        boolean first = true;
        for (Event event : events) {
            if (!first) {
                statement.append(",\n");
            }
            statement.append("(?, ?, ?, ?, cast(? as json))");
            values.add(event.getStreamId());
            values.add(event.getType());
            values.add(event.getCorrelationId());
            values.add(event.getSequence());
            values.add(event.getPayload());
            first = false;
        }

        queries.insert(statement.toString(), keyHolder, values.toArray());
        return keyHolder.stream().map(k -> (Long) k.get("event_id")).collect(toList());
    }

    public Optional<Long> getLastEventId() throws EventStoreException {
        return queries.queryForOptionalObject("select max(event_id) from event_log", Long.class);
    }

    public Optional<Long> getLastSequence(UUID streamId) throws EventStoreException {
        return queries.queryForOptionalObject("select max(seq_no) from event_log where stream_id = ?",
                Long.class, streamId);
    }

    private List<Event> queryForList(String query, Object... params) throws EventStoreException {
        return queries.queryForList(query, EventLogDao::toEvent, params);
    }

    private static Event toEvent(ResultSet rs) throws SQLException {
        Event r = new Event();
        r.setEventId(rs.getLong("event_id"));
        r.setStreamId(UUID.fromString(rs.getString("stream_id")));
        r.setType(rs.getString("type"));
        r.setCorrelationId(UUID.fromString(rs.getString("correlation_id")));
        r.setSequence(rs.getLong("seq_no"));
        r.setTransactionTime(DateConverter.toOffsetDateTime(rs.getTimestamp("transaction_time")));
        r.setPayload(rs.getString("payload"));
        return r;
    }
}
