package com.oasisdigital.nges.cluster;

/**
 * Allows publishing messages to a group and subscribing to them. The event store uses it for notifications
 * about new events and status, but the group can safely be used for any kind of notifications.
 *
 */
public interface MessageGroup {

    /**
     * Publish a message to the group.
     */
    public abstract void publish(Object message);

    /**
     * Register a subscriber for messages from the group. See documentation for a particular implementation to
     * learn more about its specifics.
     */
    public abstract void registerSubscriber(Object subscriber);

    /**
     * Unregister a subscriber from the group.
     */
    public abstract void unregisterSubscriber(Object subscriber);

}
