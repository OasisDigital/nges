package com.oasisdigital.nges.event;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Represents an event in the store.
 *
 */
public class Event implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * <p>
     * Number assigned from a globally increasing sequence. Might contain holes, i.e. it is possible for
     * events with IDs 100 and 102 to exist without an event with ID 101. Event with ID N+1 must have been
     * saved after the event with ID N.
     * </p>
     * <p>
     * Automatically populated by the system on save, never empty.
     * </p>
     */
    private long eventId;

    /**
     * ID of a stream of events, for example identifying a DDD aggregeate or another kind of entity. Populated
     * by the caller on save, mandatory.
     */
    private UUID streamId;

    /**
     * Type of the event, useful for interpreting the payload etc. Populated by the caller on save.
     */
    private String type;

    /**
     * Correlation ID. Populated by the caller on save, mandatory.
     */
    private UUID correlationId;

    /**
     * Sequence number within this particular stream. Events within a stream are automatically assigned
     * consecutive numbers starting with 1, without gaps. Never empry.
     */
    private long sequence;

    /**
     * Time when the event was saved. Events saved in one transaction are assigned the exact same timestamp.
     * Automatically populated by the system on save, never empty.
     */
    private OffsetDateTime transactionTime;

    /**
     * JSON payload.
     */
    private String payload;

    public Event() {
    }

    public Event(UUID streamId, String type, UUID correlationId, String payload) {
        this.streamId = streamId;
        this.type = type;
        this.correlationId = correlationId;
        this.payload = payload;
    }

    public void setEventId(long eventId) {
        this.eventId = eventId;
    }

    public void setStreamId(UUID streamId) {
        this.streamId = streamId;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setTransactionTime(OffsetDateTime transactionTime) {
        this.transactionTime = transactionTime;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public void setCorrelationId(UUID correlationId) {
        this.correlationId = correlationId;
    }

    public long getEventId() {
        return eventId;
    }

    public UUID getStreamId() {
        return streamId;
    }

    public String getType() {
        return type;
    }

    public OffsetDateTime getTransactionTime() {
        return transactionTime;
    }

    public String getPayload() {
        return payload;
    }

    public UUID getCorrelationId() {
        return correlationId;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

}
