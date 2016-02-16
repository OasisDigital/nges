package com.oasisdigital.nges.event;

import static java.util.stream.Collectors.toList;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.JMException;
import javax.management.ObjectName;

/**
 * JMX MBean for the event store. Exposes a number of most recent events as well as the ID of the most recent
 * event.
 */
public class EventStoreStatus implements EventStoreStatusMBean {
    private static final int LATEST_EVENTS_COUNT = 10;
    private static final String OBJECT_NAME = "EventStore:name=EventStore";

    private EventStore eventStore;

    public EventStoreStatus(EventStore eventStore) {
        this.eventStore = eventStore;
    }

    public void registerMBean() throws JMException {
        ManagementFactory.getPlatformMBeanServer().registerMBean(this, new ObjectName(OBJECT_NAME));
    }

    public void unregisterMBean() throws JMException {
        ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(OBJECT_NAME));
    }

    @Override
    public List<Map<String, Object>> getLatestEvents() {
        return eventStore.getLatestEvents(LATEST_EVENTS_COUNT).stream().map(event -> {
            try {
                Map<String, Object> result = new HashMap<>();
                result.put("eventId", event.getEventId());
                result.put("streamId", event.getStreamId().toString());
                result.put("type", event.getType());
                result.put("correlationId", event.getCorrelationId());
                result.put("sequence", event.getSequence());
                result.put("transactionTime", event.getTransactionTime().toString());
                result.put("payload", event.getPayload());
                return result;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).collect(toList());
    }

    @Override
    public Long getLastEventId() {
        return eventStore.getLastEventId().orElse(null);
    }
}
