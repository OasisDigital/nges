# NGES - "Next Generation Event Store"

Copyright 2015-2016 Oasis Digital Solutions Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this project except in compliance with the License.

## Introduction

NGES is a minimalist event store, in the form of an embeddable Java library
connecting to PostgreSQL database. Features include:

* Saving events grouped into streams. Every event is given a global sequential ID as
  well as sequence-local sequence number.
* Querying - events for all streams or one particular stream in sequence.
* Notifications - plug in to JGroups cluster to get notifications every time an event
  is saved. The cluster can also be used for any application-specific messages.
* Concurrency control:
  * Optimistic: version-based, preventing concurrent updates on the same stream
  * Pessimistic: lease-based locking, for arbitrary scopes

## Motivation

This design, as a library rather than as a (currently more trendy) standalone (micro)
service, is motivated by deployment ease. A system of built around an event store is
critically dependent on the uptime of that event store; by depending only on PostgreSQL
as a central data store, existing system administration expertise can be used. But the
database alone does not readily accommodate efficient immediate propagation of events,
so we supplement it with JGroups as a clusterable (again, using off-the-shelf system
administration skills) mechanism to propagate changes immediately.

## Higher Level Tools

NGES can serve as an effective low-level event store for an event source ("CQRS") system;
such a system would also benefit from another layer of tools, abstracting out the numerous
common concerns in implementing projections and other major application components.
Currently such layers are not yet included in NGES, could be in the future.

## Dependencies

This project aims to have as few external dependencies as reasonably possible. It depends on:

* PostgreSQL (runtime)
* PostgreSQL JDBC driver
* JGroups
* Guava

## Usage

It's recommended to create an `EventStoreContext` to wire all the pieces together.
It comes with reasonable defaults and only needs a `DataSource`.

### Append and Query

    // Initialize context
    EventStoreContext ctx = new EventStoreContext(dataSource);
    ctx.initialize();
    EventStore eventStore = ctx.getEventStore();

    // Save an event
    UUID streamId = UUID.randomUUID();
    UUID correlationId = UUID.randomUUID();
    Event event = new Event(streamId,
                            "EventStoreDemonstrated",
                            correlationId,
                            "{\"test\": \"Any JSON payload\"}");
    eventStore.save(Arrays.asList(event), "MyStream", EventStore.NEW_STREAM);

    // Get up to 100 events after event ID 0
    eventStore.getEventsForAllStreams(0, 100);

    // Get up to 100 events for given stream ID, after sequence 0 within that stream
    eventStore.getEventsForStream(streamId, 0, 100);

    // Clean up - shut down JGroups cluster and JMX monitoring
    ctx.destroy();

### JGroups Notifications

In order to be notified about new notifications, register a handler on the `MessageGroup`.
It uses Guava (local) event bus under the hood, so the handler should have Guava's `@Subscribe` method.

    class Subscriber {
        @Subscribe
        public void on(EventUpdate eventUpdate) {
            System.out.println("Last event ID is: " + eventUpdate.getEventId());
        }
    }

    MessageGroup messageGroup = ctx.getMessageGroup();
    messageGroup.registerSubscriber(new Subscriber());

NGES publishes messages of type `EventUpdate` every few seconds, or as soon as new events are saved in the
store. However, this cluster can also be used for custom application-level messages.

    class MyApplicationSubscriber {
        @Subscribe
        public void on(NewUserRegistered event) {
            // ...
        }
    }

    messageGroup.registerSubscriber(new MyApplicationSubscriber());

    messageGroup.publish(new NewUserRegistered(userId, login));

This is a lightweight messaging solution. It doesn't offer many of the features of persistent message
queues, but it's very easy to set up, has minimal footprint and may come in handy. It is intended not
as a domain level message queue, but rather as a way to propagate information around all application
servers in the cluster.

## Database Schema

NGES uses the following database schema:

    create table event_log (
      event_id bigserial primary key,
      transaction_time timestamptz default current_timestamp,
      type varchar,
      stream_id uuid not null,
      correlation_id uuid not null,
      seq_no bigint not null,
      payload json
    );

    create index event_log_by_stream_seq on event_log(stream_id, seq_no);
    create index event_log_by_transaction_time on event_log(transaction_time);

    create table event_stream_list (
      stream_id uuid not null primary key,
      stream_type varchar not null,
      last_event_id bigint,
      last_transaction_time timestamptz,
      last_seq_no bigint
    );

    create table lease (
      lease_key varchar not null primary key,
      owner_key varchar not null,
      expiration_date timestamptz
    );

## Additional Resources

* [Linear Event Store](http://blog.oasisdigital.com/2015/cqrs-linear-event-store/) - post on Oasis Digital
blog describing benefits of using a simple, linear event store like this one.

## Demo

See nges-sample-tic-tac-toe for a complete web application based on the NGES.

## Building

In order to build the project:

1. Install PostgreSQL and create a new database (for integration tests).
2. Copy config/SAMPLEapplication.properties to config/application.properties and adjust it to match your
setup.
3. Run Gradle build task.

The schema can be found in db_schema directory. The build process applies it with Flyway using the
flywayMigrate task.

Once the configuration is in place and database has the schema installed, the tests can be ran from IDE or
with Gradle.

In order to install the project to your local Maven repository for use with other projects, run the Gradle
install task.

## Should I use this?

We built the library to support a very complex project; it has its own
suite of tests, and is also been validated thoroughly by that
applications tests. It suits our needs there, operating as a library
rather than as another service to be managed. We believe it is of
good quality, and has some very worthwhile technical merits.

With that in mind, if you are looking for a production proven event
store with more features, support services available, etc., then we
recommend the popular "Event Store", also sometimes called "Greg's Event
Store", named after Greg Young who is done so much to popularize the
merit of event sourcing:

https://geteventstore.com/

