package com.oasisdigital.nges.event;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Information about particular event stream, maintained automatically as new events are written.
 */
public class EventStream {
    private UUID streamId;
    private String streamType;
    private Long lastEventId;
    private OffsetDateTime lastTransactionTime;
    private Long lastSeqNo;

    public UUID getStreamId() {
        return streamId;
    }

    public void setStreamId(UUID streamId) {
        this.streamId = streamId;
    }

    public String getStreamType() {
        return streamType;
    }

    public void setStreamType(String streamType) {
        this.streamType = streamType;
    }

    public Long getLastEventId() {
        return lastEventId;
    }

    public void setLastEventId(Long lastEventId) {
        this.lastEventId = lastEventId;
    }

    public OffsetDateTime getLastTransactionTime() {
        return lastTransactionTime;
    }

    public void setLastTransactionTime(OffsetDateTime lastTransactionTime) {
        this.lastTransactionTime = lastTransactionTime;
    }

    public Long getLastSeqNo() {
        return lastSeqNo;
    }

    public void setLastSeqNo(Long lastSeqNo) {
        this.lastSeqNo = lastSeqNo;
    }

}
