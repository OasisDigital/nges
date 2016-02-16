package com.oasisdigital.nges.event;

import com.google.common.eventbus.Subscribe;
import com.oasisdigital.nges.cluster.EventUpdate;

public class TestSubscriber {
    private long lastEvent;

    @Subscribe
    public void eventsPublished(EventUpdate eventUpdate) {
        this.lastEvent = eventUpdate.getEventId();
    }

    public long getLastEvent() {
        return lastEvent;
    }

    public void reset() {
        lastEvent = 0;
    }

}
