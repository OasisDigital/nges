package com.oasisdigital.nges.event;

import com.google.common.eventbus.Subscribe;
import com.oasisdigital.nges.cluster.EventUpdate;

public class Demo {
    public static void main(String[] args) {
        // PGPoolingDataSource dataSource = new PGPoolingDataSource();
        // dataSource.setUrl("jdbc:postgresql://localhost/tic_tac_toe");
        // dataSource.setUser("konrad");
        // dataSource.setPassword("password");
        //
        // // Initialize context
        // EventStoreContext ctx = new EventStoreContext(dataSource);
        // ctx.initialize();
        // EventStore eventStore = ctx.getEventStore();
        //
        // // Save an event
        // UUID streamId = UUID.randomUUID();
        // UUID correlationId = UUID.randomUUID();
        // Event event = new Event(streamId, "EventStoreDemonstrated", correlationId,
        // "{\"test\": \"Any JSON payload\"}");
        //
        // MessageGroup messageGroup = ctx.getMessageGroup();
        // messageGroup.registerSubscriber(new Subscriber());
        // messageGroup
        // .publish("Hello");
        //
        // eventStore.save(Arrays.asList(event), "MyStream", EventStore.NEW_STREAM);
        //
        // // Get up to 100 events after event ID 0
        // eventStore.getEventsForAllStreams(0, 100);
        //
        // // Get up to 100 events for given stream ID, after sequence 0 within that stream
        // eventStore.getEventsForStream(streamId, 0, 100);
        //
        // // Clean up - shut down JGroups cluster and JMX monitoring
        // ctx.destroy();
    }

    static class Subscriber {
        @Subscribe
        public void on(EventUpdate eventUpdate) {
            System.out.println("Last event ID is: " + eventUpdate.getEventId());
        }
    }
}
