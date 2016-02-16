package com.oasisdigital.nges.event.internal;

import com.oasisdigital.nges.cluster.EventUpdate;
import com.oasisdigital.nges.cluster.MessageGroup;

/**
 * Publishes notifications to {@link MessageGroup} about the ID of the most recent event ID. Prevents
 * publishing a number that is before the last published one in order to avoid the stream from going backwards
 * in case of concurrent calls (e.g. from heartbeat and event store).
 */
public class EventStoreStatusPublisher {
    private final MessageGroup messageGroup;

    private long lastEventId = 0;

    public EventStoreStatusPublisher(MessageGroup messageGroup) {
        this.messageGroup = messageGroup;
    }

    public void publishLastEventIfChanged(long eventId) {
        // It's OK to repeat the message (once could be lost, or a listener might join late), but the clock
        // cannot go backwards.
        if (lastEventId <= eventId) {
            messageGroup.publish(new EventUpdate(eventId));
            lastEventId = eventId;
        }
    }
}
