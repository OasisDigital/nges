package com.oasisdigital.nges.event;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Interface for the event store. Supports appending and querying events, as well as some auxiliary queries
 * about event store status.
 */
public interface EventStore extends LeaseManager {

    /**
     * <p>
     * Use as lastSequence for {@link #save(List, String, long)}.
     * </p>
     *
     * <p>
     * Create the stream if it doesn't exist yet. Auto-generate the sequence number for new events. Fail on
     * concurrent update. A call to save with this value cannot perform optimistic concurrency control.
     * </p>
     */
    public static final long AUTO_GENERATE_SEQUENCE = -1;

    /**
     * <p>
     * Use as lastSequence for {@link #save(List, String, long)}.
     * </p>
     *
     * <p>
     * Create new stream. Fail if it already exists.
     * </p>
     */
    // This value is intentionally 0, so that the first inserted sequence is 1.
    public static final long NEW_STREAM = 0;

    /**
     * Append events to the event log.
     *
     * @param events
     *            New events to append to the log.
     * @param streamType
     *            Name of the stream type, used for bookkeeping and preventing concurrent creation of another
     *            stream type with the same ID.
     * @param lastSequence
     *            <p>
     *            The expected sequence number of the last event within the stream, or one of
     *            {@link #AUTO_GENERATE_SEQUENCE} or {@link #NEW_STREAM}. If the value is different from
     *            {@link #AUTO_GENERATE_SEQUENCE}, the number is used for optimistic concurrency control and
     *            this method will throw an exception if it does not match the current value in database.
     *            </p>
     *            <p>
     *            The way to use it is: Query the event store to reconstruct state, remembering the sequence
     *            number of the last event. Calculate the new state (i.e. new events to append to the store)
     *            and pass the remembered sequence number for saving. The store will use that number to detect
     *            concurrent modification of the stream and throw {@link EventStoreConflict}.
     *            </p>
     * @return IDs of the newly saved events, in the same order as the input list.
     * @throws EventStoreConflict
     */
    List<Long> save(List<Event> events, String streamType, long lastSequence) throws EventStoreException;

    /**
     * Like {@link #save(List, String, long)}, but it only saves the events if all of the given lease keys are
     * owned by the given owner. If any of the leases is missing, no events are saved and this method returns
     * an empty list.
     *
     * @see #lease(String, String, long)
     */
    List<Long> save(List<Event> events, String streamType, long lastSequence, Collection<String> leaseKeys,
            String leaseOwnerKey) throws EventStoreException;

    /**
     * Get a single event by ID. Throws {@link EventStoreException} if the event does not exist.
     */
    Event getEvent(long eventId) throws EventStoreException;

    /**
     * Get a number of most recent events.
     */
    List<Event> getLatestEvents(int limit) throws EventStoreException;

    /**
     * Get a number of events after given event ID, sorted by event ID.
     */
    List<Event> getEventsForAllStreams(long afterEventId, int limit) throws EventStoreException;

    /**
     * Get a number of events for particular stream after given sequence number.
     */
    List<Event> getEventsForStream(UUID streamId, long afterSequenceId, int limit) throws EventStoreException;

    /**
     * Get the ID of the most recent event.
     */
    Optional<Long> getLastEventId() throws EventStoreException;

    /**
     * Get the last sequence number for a stream.
     */
    Optional<Long> getLastSequence(UUID streamId) throws EventStoreException;

    /**
     * Get the number of event streams in the store.
     */
    long getStreamCount(String streamType) throws EventStoreException;

    /**
     * Get information about a particular stream. Throws {@link EventStoreException} if the stream does not
     * exist.
     */
    EventStream findByStreamId(UUID id) throws EventStoreException;
}
