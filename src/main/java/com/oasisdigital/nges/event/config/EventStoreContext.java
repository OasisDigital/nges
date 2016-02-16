package com.oasisdigital.nges.event.config;

import javax.sql.DataSource;

import com.oasisdigital.nges.cluster.JGroupsMessageGroup;
import com.oasisdigital.nges.cluster.MessageGroup;
import com.oasisdigital.nges.event.EventStoreStatus;
import com.oasisdigital.nges.event.EventStoreStatusHeartbeat;
import com.oasisdigital.nges.event.EventStore;
import com.oasisdigital.nges.event.internal.EventStoreStatusPublisher;
import com.oasisdigital.nges.event.jdbc.JdbcEventStore;

/**
 * <p>
 * This class is the main entry point to the event store. While the pieces are still perfectly usable without
 * it, it simplifies the initialization and wiring things together.
 * </p>
 *
 * <p>
 * It creates the event store along with JGroups {@link MessageGroup} and {@link EventStoreStatusHeartbeat}
 * (publishing event status notifications at regular interval). The message group will use
 * {@link #DEFAULT_JGROUPS_CLUSTER_NAME} and {@link #DEFAULT_JGROUPS_PORT}. The heartbeat will run with
 * {@link #DEFAULT_HEARTBEAT_DELAY} and {@link #DEFAULT_HEARTBEAT_INTERVAL}.
 * </p>
 *
 * <p>
 * The configuration can be changed after creation, as long as it's done before calling {@link #initialize()}.
 * </p>
 *
 * <p>
 * It is necessary to call {@link #initialize()} before the store can be used.
 * </p>
 * *
 */
public class EventStoreContext {
    public static final String DEFAULT_JGROUPS_CLUSTER_NAME = "EventStore";
    public static final int DEFAULT_JGROUPS_PORT = 47123;
    public static final long DEFAULT_HEARTBEAT_DELAY = 0;
    public static final long DEFAULT_HEARTBEAT_INTERVAL = 5000;

    private final JGroupsMessageGroup messageGroup;
    private final JdbcEventStore eventStore;
    private final EventStoreStatusPublisher statusPublisher;
    private final EventStoreStatusHeartbeat heartbeat;
    private final EventStoreStatus jmx;

    public EventStoreContext(DataSource dataSource) {
        this.messageGroup = new JGroupsMessageGroup();
        this.statusPublisher = new EventStoreStatusPublisher(messageGroup);
        this.eventStore = new JdbcEventStore(dataSource, statusPublisher);
        this.heartbeat = new EventStoreStatusHeartbeat(eventStore, statusPublisher);
        this.jmx = new EventStoreStatus(eventStore);

        configureMessageGroup(DEFAULT_JGROUPS_CLUSTER_NAME, DEFAULT_JGROUPS_PORT);
        configureHeartbeat(DEFAULT_HEARTBEAT_DELAY, DEFAULT_HEARTBEAT_INTERVAL);
    }

    /**
     * Must not be called after {@link #initialize()}.
     */
    public void configureMessageGroup(String jgroupsClusterName, int jgroupsMulticastPort) {
        this.messageGroup.setClusterName(jgroupsClusterName);
        this.messageGroup.setMulticastPort(jgroupsMulticastPort);
    }

    /**
     * Must not be called after {@link #initialize()}.
     *
     * @param defaultHeartbeatDelay
     *            delay (in milliseconds) before querying the event store and publishing its status for the
     *            first time
     * @param defaultHeartbeatInterval
     *            interval (in milliseconds) of polling the event store and publishing its status
     */
    public void configureHeartbeat(long initialDelayMs, long pollingIntervalMs) {
        this.heartbeat.setInitialDelayMs(initialDelayMs);
        this.heartbeat.setPollingIntervalMs(pollingIntervalMs);
    }

    /**
     * <p>
     * Creates thread pools for message group and heartbeat. Initializes JGroups cluster. Registers JMX MBean
     * for event store status.
     * </p>
     *
     * <p>
     * It is mandatory to call this method prior to using the event store.
     * </p>
     *
     * <p>
     * This method may take a while. For example JGroups initialization may take a few seconds. It may be a
     * good idea to run it in a background thread to speed up application start.
     * </p>
     */
    public void initialize() {
        try {
            messageGroup.initialize();
            heartbeat.initialize();
            jmx.registerMBean();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Shut down JGroups cluster and thread pools, unregister JMX MBean.
     */
    public void destroy() {
        try {
            messageGroup.destroy();
            heartbeat.destroy();
            jmx.unregisterMBean();
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public MessageGroup getMessageGroup() {
        return messageGroup;
    }

    public EventStore getEventStore() {
        return eventStore;
    }
}
