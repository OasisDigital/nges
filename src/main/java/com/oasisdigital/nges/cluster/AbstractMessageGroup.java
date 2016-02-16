package com.oasisdigital.nges.cluster;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.Subscribe;

/**
 * Base abstract class for the message group, using Guava {@link AsyncEventBus} for notifying subscribers
 * about new messages. Subscribers are plugged in directly to the bus, receiving messages on methods annotated
 * with {@link Subscribe}. For more information see
 * <a href="https://github.com/google/guava/wiki/EventBusExplained">Guava docs</a>.
 */
abstract public class AbstractMessageGroup implements MessageGroup {

    protected ScheduledExecutorService executor;
    protected AsyncEventBus eventBus;

    @Override
    public void registerSubscriber(Object subscriber) {
        eventBus.register(subscriber);
    }

    /**
     * @throws IllegalArgumentException
     *             if the subscriber is not registered with the event bus.
     */
    @Override
    public void unregisterSubscriber(Object subscriber) {
        eventBus.unregister(subscriber);
    }

    synchronized public void initialize() throws Exception {
        if (executor != null) {
            throw new IllegalStateException("Already initialized");
        }
        executor = Executors.newScheduledThreadPool(1);
        eventBus = new AsyncEventBus(executor);
    }

    synchronized public void destroy() {
        executor.shutdownNow();
    }

}
