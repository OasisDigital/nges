package com.oasisdigital.nges.event;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.oasisdigital.nges.cluster.EventUpdate;
import com.oasisdigital.nges.cluster.MessageGroup;
import com.oasisdigital.nges.event.internal.EventStoreStatusPublisher;
import com.oasisdigital.nges.event.util.LogThrottle;

/**
 * Periodically publishes {@link MessageGroup} notifications about the most recent event ID in the store. The
 * notifications are of {@link EventUpdate} type.
 *
 */
public class EventStoreStatusHeartbeat {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final EventStore eventStore;
    private final EventStoreStatusPublisher publisher;

    private long initialDelayMs;
    private long pollingIntervalMs;

    private ScheduledExecutorService executor;

    public EventStoreStatusHeartbeat(EventStore eventStore, EventStoreStatusPublisher publisher) {
        this.eventStore = eventStore;
        this.publisher = publisher;
    }

    synchronized public void setInitialDelayMs(long initialDelayMs) {
        Preconditions.checkArgument(initialDelayMs >= 0);
        Preconditions.checkState(executor == null, "Must be called before initialize()");

        this.initialDelayMs = initialDelayMs;
    }

    synchronized public void setPollingIntervalMs(long pollingIntervalMs) {
        Preconditions.checkArgument(pollingIntervalMs >= 0);
        Preconditions.checkState(executor == null, "Must be called before initialize()");

        this.pollingIntervalMs = pollingIntervalMs;
    }

    synchronized public void initialize() {
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(catchAllAndLogWithThrottle(this::publishLastEvent), initialDelayMs,
                pollingIntervalMs, TimeUnit.MILLISECONDS);
    }

    private Runnable catchAllAndLogWithThrottle(Runnable r) {
        LogThrottle throttle = new LogThrottle(1, TimeUnit.HOURS);
        return () -> {
            try {
                r.run();
            } catch (RuntimeException e) {
                if (throttle.throttle(e)) {
                    log.error("Execution failed: " + e);
                } else {
                    log.error("Execution failed", e);
                }
            }
        };
    }

    public void destroy() {
        executor.shutdownNow();
    }

    private void publishLastEvent() {
        eventStore.getLastEventId().ifPresent(publisher::publishLastEventIfChanged);
    }
}
