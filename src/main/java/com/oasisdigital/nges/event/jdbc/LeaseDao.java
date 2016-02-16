package com.oasisdigital.nges.event.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.google.common.base.Strings;
import com.oasisdigital.nges.event.EventStoreConflict;
import com.oasisdigital.nges.event.EventStream;
import com.oasisdigital.nges.event.Lease;
import com.oasisdigital.nges.event.EventStoreException;

class LeaseDao {
    private final JdbcQueries queries;

    public LeaseDao(ConnectionSource connectionSource) {
        this.queries = new JdbcQueries(connectionSource);
    }

    /**
     *
     * @param streamId
     * @param lastEventId
     *            ID of the last (new) event for the stream
     * @param lastSequence
     *            sequence number of the last (new) event for the stream
     * @param expectedSequence
     *            sequence number that the update should be applied to; use "0" to indicate a new sequence
     * @throws EventStoreException
     */
    public void update(UUID streamId, long lastEventId, long lastSequence, long expectedSequence)
            throws EventStoreException {
        // @formatter:off
        int updated = queries.update(
                "update event_stream_list set "
              + "  last_event_id = ?, "
              + "  last_transaction_time = current_timestamp, "
              + "  last_seq_no = ? "
              + "where stream_id = ? and coalesce(last_seq_no, 0) = ?",
                lastEventId,
                lastSequence,
                streamId,
                expectedSequence);
        // @formatter:on
        if (updated == 0) {
            throw new EventStoreConflict("Uninitialized stream or concurrent modification: " + streamId);
        }
    }

    public EventStream findByStreamId(UUID id) throws EventStoreException {
        return queries.queryForObject("select * from event_stream_list where stream_id = ?",
                LeaseDao::toEventStream, id);
    }

    public void initStream(UUID streamId, String streamType) throws EventStoreException {
        boolean streamCreated = initStreamIfNeeded(streamId, streamType);
        if (!streamCreated) {
            throw new EventStoreConflict("The stream already exists: " + streamId + " of type " + streamType);
        }
    }

    /**
     * Create record for a new stream if it doesn't exist.
     *
     * @return true if the stream did not exist and a new record has just been created
     * @throws EventStoreException
     */
    public boolean initStreamIfNeeded(UUID streamId, String streamType) throws EventStoreException {
        try {
            // @formatter:off
            int affectedRows = queries.insert(
                    "insert into event_stream_list(stream_id, stream_type) "
                  + "select ?, ? "
                  + "where not exists (select stream_id from event_stream_list where stream_id = ? and stream_type = ?)",
                    streamId, streamType, streamId, streamType);
            // @formatter:on
            return affectedRows == 1;
        } catch (UniqueViolation ex) {
            throw new EventStoreConflict("Another stream with this key already exists");
        }
    }

    public long getStreamCount(String streamType) throws EventStoreException {
        return queries.queryForObject("select count(*) from event_stream_list where stream_type = ?",
                Long.class, streamType);
    }

    private static EventStream toEventStream(ResultSet resultSet) throws SQLException {
        EventStream result = new EventStream();
        result.setStreamId(UUID.fromString(resultSet.getString("stream_id")));
        result.setStreamType(resultSet.getString("stream_type"));
        result.setLastEventId((Long) resultSet.getObject("last_event_id"));
        result.setLastTransactionTime(
                DateConverter.toOffsetDateTime(resultSet.getTimestamp("last_transaction_time")));
        result.setLastSeqNo((Long) resultSet.getObject("last_seq_no"));
        return result;
    }

    public Lease createOrRenewLease(String leaseKey, String ownerKey, long leaseDurationMs) {
        // @formatter:off
        int rows = queries.update("insert into lease(lease_key, owner_key, expiration_date) "
                                + "select ?, ?, current_timestamp + cast(? || 'ms' as interval)"
                                + "where not exists (select * from lease where lease_key = ?)",
                                  leaseKey, ownerKey, leaseDurationMs, leaseKey);
        // @formatter:on

        if (rows == 1) {
            return getLease(leaseKey);
        }

        // @formatter:off
        rows = queries.update("update lease set "
                            + "owner_key = ?, "
                            + "expiration_date = current_timestamp + cast(? || 'ms' as interval) "
                            + "where lease_key = ? and (owner_key = ? or expiration_date < current_timestamp)",
                              ownerKey, leaseDurationMs, leaseKey, ownerKey);
        // @formatter:on

        return getLease(leaseKey);
    }

    public Lease renewLease(String leaseKey, String ownerKey, long leaseDurationMs) {
        // @formatter:off
        queries.update("update lease set "
                     + "owner_key = ?, "
                     + "expiration_date = current_timestamp + cast(? || 'ms' as interval) "
                     + "where lease_key = ? and owner_key = ?",
                       ownerKey, leaseDurationMs, leaseKey, ownerKey);
        // @formatter:on

        return getLease(leaseKey);
    }

    public Lease getLease(String leaseKey) {
        return queries.queryForObject("select * from lease where lease_key = ?", LeaseDao::toLease, leaseKey);
    }

    private static Lease toLease(ResultSet rs) throws SQLException {
        Lease lease = new Lease();
        lease.setLeaseKey(rs.getString("lease_key"));
        lease.setOwnerKey(rs.getString("owner_key"));
        lease.setExpirationDate(DateConverter.toOffsetDateTime(rs.getTimestamp("expiration_date")));
        return lease;
    }

    public boolean verifyLeases(Collection<String> leaseKeys, String leaseOwnerKey) {
        String query = "select count(*) from lease where lease_key in ("
                + Strings.repeat(",?", leaseKeys.size()).substring(1) + ") "
                + "and owner_key = ? and expiration_date > current_timestamp";
        List<Object> params = new ArrayList<>(leaseKeys);
        params.add(leaseOwnerKey);
        Optional<Long> activeLeases = queries.queryForOptionalObject(query, Long.class, params.toArray());
        return activeLeases.equals(Optional.of((long) leaseKeys.size()));
    }

    public void release(String leaseKey, String ownerKey) {
        queries.update(
                "update lease set expiration_date = current_timestamp where lease_key = ? and owner_key = ?",
                leaseKey, ownerKey);
    }

}
