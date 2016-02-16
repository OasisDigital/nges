package com.oasisdigital.nges.event.jdbc;

import static java.util.Arrays.asList;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import com.oasisdigital.nges.event.Event;
import com.oasisdigital.nges.event.EventStore;
import com.oasisdigital.nges.event.EventStoreConflict;
import com.oasisdigital.nges.event.EventStoreException;
import com.oasisdigital.nges.event.EventStream;
import com.oasisdigital.nges.event.Lease;
import com.oasisdigital.nges.event.internal.EventStoreStatusPublisher;

public class JdbcEventStore implements EventStore {
    private final ConnectionSource connectionSource;
    private final EventLogDao eventLog;
    private final EventStreamListDao streamList;
    private final LeaseDao lease;
    private final EventStoreStatusPublisher statusPublisher;

    public JdbcEventStore(DataSource dataSource, EventStoreStatusPublisher statusPublisher) {
        this.connectionSource = new ConnectionSource(dataSource);
        this.eventLog = new EventLogDao(connectionSource);
        this.streamList = new EventStreamListDao(connectionSource);
        this.lease = new LeaseDao(connectionSource);
        this.statusPublisher = statusPublisher;
    }

    @Override
    public List<Long> save(List<Event> events, String streamType, long lastSequence)
            throws EventStoreConflict {
        return save(events, streamType, lastSequence, Collections.emptyList(), "");
    }

    @Override
    public List<Long> save(List<Event> events, String streamType, long lastSequence,
            Collection<String> leaseKeys, String leaseOwnerKey) throws EventStoreException {
        if (countStreams(events) > 1) {
            throw new IllegalArgumentException("All events should correspond to the same stream");
        }
        if (!asList(AUTO_GENERATE_SEQUENCE, NEW_STREAM).contains(lastSequence) && lastSequence <= 0) {
            throw new IllegalArgumentException("Invalid lastSequence");
        }
        if (events.isEmpty()) {
            return Collections.emptyList();
        } else {
            try {
                List<Long> ids = connectionSource.inTransaction(conn -> saveInTransaction(events, streamType,
                        lastSequence, leaseKeys, leaseOwnerKey));
                if (!ids.isEmpty()) {
                    postEventUpdate(ids.get(ids.size() - 1));
                }
                return ids;
            } catch (Exception ex) {
                if (ex instanceof RuntimeException) {
                    throw (RuntimeException) ex;
                } else if (ex instanceof EventStoreConflict) {
                    throw (EventStoreConflict) ex;
                } else {
                    // Should not happen - no other checked exceptions in saveInTransaction
                    throw new IllegalStateException(ex);
                }
            }
        }
    }

    private List<Long> saveInTransaction(List<Event> events, String streamType, long lastSequence,
            Collection<String> leaseKeys, String leaseOwnerKey) throws EventStoreException {
        return connectionSource.<List<Long>> withConnection(conn -> {
            conn.setAutoCommit(false);
            // Serialize writes. Needed when:
            //
            // 1. a new stream might be created (to prevent concurrent writes to event_stream_list)
            //
            // 2. saving batches of more than 1 event at a time so they get consistent ID blocks. Otherwise
            // it would lead to anomalies in read models, because an event with ID N could be committed
            // *after* an event with ID N+1 from another stream.
            lockUpdates();
            if (!verifyLease(leaseKeys, leaseOwnerKey)) {
                return Collections.emptyList();
            }
            UUID streamId = events.get(0).getStreamId();

            long _lastSequence;
            if (lastSequence == AUTO_GENERATE_SEQUENCE) {
                _lastSequence = getLastSequence(streamId).orElse(0L);
            } else {
                _lastSequence = lastSequence;
            }
            if (_lastSequence == 0) { // NEW_STREAM or AUTO_GENERATE_SEQUENCE with none in DB
                streamList.initStream(streamId, streamType);
            }
            for (int i = 0; i < events.size(); i++) {
                events.get(i).setSequence(_lastSequence + i + 1);
            }
            List<Long> ids = eventLog.save(events);
            streamList.update(streamId, ids.get(ids.size() - 1), _lastSequence + events.size(),
                    _lastSequence);
            return ids;
        });
    }

    private boolean verifyLease(Collection<String> leaseKeys, String leaseOwnerKey) {
        if (leaseKeys.isEmpty()) {
            return true;
        } else {
            return lease.verifyLeases(leaseKeys, leaseOwnerKey);
        }
    }

    private void lockUpdates() {
        new JdbcQueries(connectionSource).query("select pg_advisory_xact_lock(?)", rs -> null, 0xCAFEBABE);
    }

    private long countStreams(List<Event> events) {
        return events.stream().map(Event::getStreamId).distinct().count();
    }

    @Override
    public Event getEvent(long eventId) throws EventStoreException {
        return eventLog.getEvent(eventId);
    }

    @Override
    public List<Event> getLatestEvents(int limit) throws EventStoreException {
        return eventLog.getLatestEvents(limit);
    }

    @Override
    public List<Event> getEventsForAllStreams(long afterEventId, int limit) throws EventStoreException {
        return eventLog.getEventsForAllStreams(afterEventId, limit);
    }

    @Override
    public List<Event> getEventsForStream(UUID streamId, long afterSequenceId, int limit)
            throws EventStoreException {
        return eventLog.getEventsForStream(streamId, afterSequenceId, limit);
    }

    @Override
    public Optional<Long> getLastEventId() throws EventStoreException {
        return eventLog.getLastEventId();
    }

    @Override
    public Optional<Long> getLastSequence(UUID streamId) throws EventStoreException {
        return eventLog.getLastSequence(streamId);
    }

    @Override
    public long getStreamCount(String streamType) throws EventStoreException {
        return streamList.getStreamCount(streamType);
    }

    @Override
    public EventStream findByStreamId(UUID id) throws EventStoreException {
        return streamList.findByStreamId(id);
    }

    @Override
    public Lease lease(String leaseKey, String ownerKey, long leaseDurationMs) {
        return connectionSource.inTransaction(conn -> {
            lockUpdates();
            return lease.createOrRenewLease(leaseKey, ownerKey, leaseDurationMs);
        });
    }

    @Override
    public Lease renewLease(String leaseKey, String ownerKey, long leaseDurationMs) {
        return connectionSource.inTransaction(conn -> {
            lockUpdates();
            return lease.renewLease(leaseKey, ownerKey, leaseDurationMs);
        });
    }

    @Override
    public void release(String leaseKey, String ownerKey) {
        lease.release(leaseKey, ownerKey);
    }

    private void postEventUpdate(long lastId) {
        statusPublisher.publishLastEventIfChanged(lastId);
    }
}
