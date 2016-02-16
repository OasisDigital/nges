package com.oasisdigital.nges.cluster;

/**
 * In-memory {@link MessageGroup} implementation. May be useful for development or when there is exactly one
 * process interested in the event store.
 */
public class InMemoryMessageGroup extends AbstractMessageGroup implements MessageGroup {

    @Override
    public void publish(Object message) {
        if (eventBus == null) {
            throw new IllegalStateException("Message group not initialized, call initialize() first.");
        }
        eventBus.post(message);
    }

}
