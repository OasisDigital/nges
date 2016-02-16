package com.oasisdigital.nges.cluster;

import java.io.Serializable;

/**
 * Message with the current last event ID in the store.
 *
 */
public class EventUpdate implements Serializable {
    private static final long serialVersionUID = 4220177459255722476L;

    private long eventId;

    public EventUpdate(long eventId) {
        this.eventId = eventId;
    }

    public long getEventId() {
        return eventId;
    }
}
