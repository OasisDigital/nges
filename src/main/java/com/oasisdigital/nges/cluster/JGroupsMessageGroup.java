package com.oasisdigital.nges.cluster;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * JGroups-based MessageGroup.
 */
public class JGroupsMessageGroup extends AbstractMessageGroup {

    private static final Logger log = LoggerFactory.getLogger(JGroupsMessageGroup.class);

    private static final long INITIALIZATION_TIMEOUT_SECONDS = 15;

    private JChannel channel;

    private String clusterName;

    private int multicastPort;

    @Override
    synchronized public void initialize() throws Exception {
        super.initialize();

        CompletableFuture<JChannel> channelFuture = CompletableFuture.supplyAsync(() -> {
            // Put this where JGroups can see it:
            System.setProperty("jgroups.udp.mcast_port", String.valueOf(multicastPort));

            try {
                JChannel channel = new JChannel("cluster.xml");
                channel.setName("EventsAndStatus");
                channel.setDiscardOwnMessages(false); // Also the default, this is documentation.
                channel.setReceiver(new ReceiverAdapter() {
                    @Override
                    public void receive(Message msg) {
                        eventBus.post(msg.getObject());
                    }
                });
                channel.connect(clusterName);
                log.info("JGroups cluster initialization complete");
                return channel;
            } catch (Exception e) {
                throw new RuntimeException("Unable to initialize JGroups", e);
            }
        });

        executor.schedule(
                () -> channelFuture.completeExceptionally(new TimeoutException(
                        "Failed to connect in " + INITIALIZATION_TIMEOUT_SECONDS + " seconds")),
                INITIALIZATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        this.channel = channelFuture.get();
    }

    @Override
    synchronized public void destroy() {
        try {
            if (channel != null) {
                channel.close();
            }
        } finally {
            super.destroy();
        }
    }

    @Override
    public void publish(Object message) {
        if (channel == null) {
            throw new IllegalStateException("Message group not initialized, call initialize() first.");
        }
        try {
            channel.send(new Message(null, message));
        } catch (ExecutionException ex) {
            throw new RuntimeException("Cluster initialization failed", ex);
        } catch (Exception ex) {
            // Channel send can throw various exceptions, but will typically do so only in an extreme case of
            // a network card disappearing or similar. There is nothing we can do about it, we will simply log
            // and move on.
            //
            // Throwing an exception to the caller would not help.
            //
            // In the case of an event being published, the event has already been published so the fact that
            // we cannot successfully alert listeners cannot change that fact.
            //
            // In the case of projection status, the projection status is already changed and again we cannot
            // prevent it from happening just because we were unable to announce it to listeners.

            log.error("JGroups channel send error", ex);
        }
    }

    public void setClusterName(String clusterName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(clusterName));
        Preconditions.checkState(channel == null, "Must be called before initialize()");

        this.clusterName = clusterName;
    }

    public void setMulticastPort(int multicastPort) {
        Preconditions.checkArgument(multicastPort > 0);
        Preconditions.checkState(channel == null, "Must be called before initialize()");

        this.multicastPort = multicastPort;
    }

    // TODO Consider for the latest event ID:
    // http://www.jgroups.org/manual/html/user-building-blocks.html#CounterService

}
