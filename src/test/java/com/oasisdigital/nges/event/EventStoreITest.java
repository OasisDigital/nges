package com.oasisdigital.nges.event;

import static com.jayway.awaitility.Awaitility.await;
import static com.oasisdigital.nges.event.EventStore.AUTO_GENERATE_SEQUENCE;
import static com.oasisdigital.nges.event.EventStore.NEW_STREAM;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.oasisdigital.nges.cluster.MessageGroup;
import com.oasisdigital.nges.event.EventStoreConflict;
import com.oasisdigital.nges.event.EventStream;
import com.oasisdigital.nges.event.Lease;
import com.oasisdigital.nges.event.Event;
import com.oasisdigital.nges.event.EventStore;
import com.oasisdigital.nges.event.EventStoreException;
import com.oasisdigital.nges.event.config.EventStoreContext;
import com.oasisdigital.nges.event.jdbc.BaseITest;

@Test(groups = TestGroups.INTEGRATION)
public class EventStoreITest extends BaseITest {
    private static Random random = new Random();

    private static EventStoreContext context;
    private static EventStore eventStore;
    private static MessageGroup messageGroup;
    private static TestSubscriber subscriber;

    private UUID streamId;

    @BeforeClass
    public void setUpClass() throws Exception {
        context = new EventStoreContext(dataSource);
        context.initialize();
        eventStore = context.getEventStore();
        messageGroup = context.getMessageGroup();

        subscriber = new TestSubscriber();
        messageGroup.registerSubscriber(subscriber);
    }

    @BeforeMethod
    public void setUpMethod() {
        streamId = randomUUID();
    }

    @AfterClass
    public void tearDownClass() throws Exception {
        context.destroy();
    }

    //
    // EVENT PERSISTENCE
    //

    @Test
    public void shouldPersistAndReadEvents() throws Exception {
        Event saving = new Event(streamId, "TextAppended", randomUUID(), "{\"text\": \"abc\"}");

        long eventId = save(saving);

        List<Event> events = eventStore.getEventsForStream(streamId, 0, 100);

        assertThat(events, hasSize(1));
        Event saved = events.get(0);
        assertThat(saved.getEventId(), is(eventId));
        assertThat(saved.getTransactionTime(), is(after(OffsetDateTime.now().minusSeconds(2))));
        assertThat(saved.getStreamId(), is(streamId));
        assertThat(saved.getPayload(), is(saving.getPayload()));
        assertThat(saved.getSequence(), is(1L));
    }

    @Test
    public void shouldUpdateStreamIndex() throws Exception {
        EventStream stream;

        long eventId = save(textAppended("a"));
        Event event = eventStore.getEvent(eventId);

        stream = eventStore.findByStreamId(streamId);
        assertThat(stream.getStreamId(), is(streamId));
        assertThat(stream.getStreamType(), is("Recipe"));
        assertThat(stream.getLastEventId(), is(eventId));
        assertThat(stream.getLastTransactionTime(), is(event.getTransactionTime()));
        assertThat(stream.getLastSeqNo(), is(1L));

        eventId = save(asList(textAppended("b"), textAppended("c")), 1).get(1);
        event = eventStore.getEvent(eventId);

        stream = eventStore.findByStreamId(streamId);
        assertThat(stream.getStreamId(), is(streamId));
        assertThat(stream.getStreamType(), is("Recipe"));
        assertThat(stream.getLastEventId(), is(eventId));
        assertThat(stream.getLastTransactionTime(), is(event.getTransactionTime()));
        assertThat(stream.getLastSeqNo(), is(3L));
    }

    @Test
    public void shouldNotifyListenersWhenSavingEvents() throws Exception {
        List<Long> ids = save(asList(textAppended("foo"), textAppended("bar")));
        long lastId = ids.get(1);
        await().atMost(5, TimeUnit.SECONDS).until(() -> subscriber.getLastEvent() == lastId);
    }

    @Test
    public void shouldReturnEventsInOrder() throws Exception {
        long id1 = save(textAppended("42"));
        long id2 = save(textAppended("43"), 1);
        long id3 = save(textAppended("44"), 2);

        List<Event> events = eventStore.getEventsForStream(streamId, 0, 100);

        assertThat(getIds(events), contains(id1, id2, id3));
    }

    @Test
    public void shouldSupportQueryingAfterGivenEvent() throws Exception {
        @SuppressWarnings("unused")
        long id1 = save(textAppended("42"));
        long id2 = save(textAppended("43"), 1);
        long id3 = save(textAppended("44"), 2);

        List<Event> events = eventStore.getEventsForStream(streamId, 1, 100);

        assertThat(getIds(events), contains(id2, id3));
    }

    @Test
    public void shouldAssignSequenceNumbersToEvents() throws Exception {
        save(textAppended("A"));
        save(textAppended("B"), 1);
        save(asList(textAppended("C"), textAppended("D")), 2);

        List<Event> events = eventStore.getEventsForStream(streamId, 0, 100);
        assertThat(getSequences(events), contains(1L, 2L, 3L, 4L));
    }

    @Test
    public void shouldSupportGeneratingSequence() throws Exception {
        save(asList(textAppended("A"), textAppended("B")), AUTO_GENERATE_SEQUENCE);
        save(asList(textAppended("C"), textAppended("D")), AUTO_GENERATE_SEQUENCE);

        List<Event> events = eventStore.getEventsForStream(streamId, 0, 100);
        assertThat(getSequences(events), contains(1L, 2L, 3L, 4L));
    }

    @Test(expectedExceptions = EventStoreConflict.class)
    public void shouldRejectConcurrentStreamCreation() throws Exception {
        // When two requests to create a stream arrive after each other (~concurrently)
        long eventId = save(textAppended("foo"), NEW_STREAM);

        // Then expect the first one to succeed
        assertThat(eventId, is(greaterThan(0L)));

        // Then expect the first one to be rejected as conflict
        save(textAppended("bar"), NEW_STREAM);
    }

    @Test(expectedExceptions = EventStoreConflict.class)
    public void shouldRejectConcurrentStreamAppending() throws Exception {
        // Given an existing stream
        save(textAppended("foo"), NEW_STREAM);

        // When appending two events concurrently (after the same "last sequence")
        long eventId = save(textAppended("bar"), 1);
        assertThat(eventId, is(greaterThan(0L)));

        // Then expect the second request to be rejected as conflict
        save(textAppended("baz"), 1);
    }

    @Test
    public void shouldSerializeConcurrentStreamAppendingWithAutoGeneratedSequence() throws Exception {
        // When appending a number of events concurrently
        IntStream.range(0, 10).parallel().forEach(i -> {
            try {
                save(textAppended(String.valueOf(i)), AUTO_GENERATE_SEQUENCE);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Then expect it to write them all successfully
        List<Event> events = eventStore.getEventsForStream(streamId, 0, 100);
        assertThat(events.size(), is(10));
    }

    @Test
    public void shouldSerializeConcurrentBatchAppendingToDifferentStreams() throws Exception {
        List<UUID> streamIds = Stream.generate(() -> UUID.randomUUID()).limit(10).collect(toList());
        // Initialize some streams
        streamIds.forEach(streamId -> save(textAppended(streamId, "-")));

        long lastEventId = eventStore.getLastEventId().orElse(0L);

        // Append to them in parallel - this is a separate call, so that we don't interfere with stream
        // initialization (which may involve locking). The goal of the test is to verify that just appending
        // to existing stream yields consistent blocks.
        // @formatter:off
        streamIds.stream()
                 .parallel()
                 .forEach(streamId -> save(asList(textAppended(streamId, "a"),
                                                  textAppended(streamId, "b"),
                                                  textAppended(streamId, "c")),
                                           1));
        // @formatter:on0
        List<Event> events = eventStore.getEventsForAllStreams(lastEventId, 100);
        assertThat(events.size(), is(30));
        List<UUID> savedStreamIdsInOrder = events.stream().map(e -> e.getStreamId()).collect(toList());
        assertThat(savedStreamIdsInOrder, consistsOfConsistentBlocks(10, 3));
    }

    //
    // LEASE
    //

    @Test
    public void shouldPreventLeaseToAnotherOwner() {
        String leaseKey = randomKey("Lease");
        String ownerKey1 = randomKey("Owner1");
        String ownerKey2 = randomKey("Owner2");

        assertThat(eventStore.lease(leaseKey, ownerKey1, 2000).getOwnerKey(), is(ownerKey1));
        assertThat(eventStore.lease(leaseKey, ownerKey2, 2000).getOwnerKey(), is(ownerKey1));
    }

    @Test
    public void shouldRenewLeaseToCurrentOwnerWithLease() {
        String leaseKey = randomKey("Lease");
        String ownerKey = randomKey("Owner");

        Lease first = eventStore.lease(leaseKey, ownerKey, 2000);
        Lease second = eventStore.lease(leaseKey, ownerKey, 2000);

        assertThat(second.getOwnerKey(), is(ownerKey));
        assertThat(second.getExpirationDate(), is(after(first.getExpirationDate())));
    }

    @Test
    public void shouldLeaseToAnotherOwnerAfterOwnerReleases() {
        String leaseKey = randomKey("Lease");
        String ownerKey1 = randomKey("Owner1");
        String ownerKey2 = randomKey("Owner2");

        assertThat(eventStore.lease(leaseKey, ownerKey1, 2000).getOwnerKey(), is(ownerKey1));
        eventStore.release(leaseKey, ownerKey1);

        assertThat(eventStore.lease(leaseKey, ownerKey2, 2000).getOwnerKey(), is(ownerKey2));
    }

    @Test
    public void shouldIgnoreReleaseFromNonOwner() {
        String leaseKey = randomKey("Lease");
        String ownerKey1 = randomKey("Owner1");
        String ownerKey2 = randomKey("Owner2");

        assertThat(eventStore.lease(leaseKey, ownerKey1, 2000).getOwnerKey(), is(ownerKey1));
        eventStore.release(leaseKey, ownerKey2);

        assertThat(eventStore.lease(leaseKey, ownerKey2, 2000).getOwnerKey(), is(ownerKey1));
    }

    @Test
    public void shouldLeaseDifferentKeysToDifferentProcesses() {
        String leaseKey1 = randomKey("Lease1");
        String leaseKey2 = randomKey("Lease2");
        String ownerKey1 = randomKey("Owner1");
        String ownerKey2 = randomKey("Owner2");

        assertThat(eventStore.lease(leaseKey1, ownerKey1, 2000).getOwnerKey(), is(ownerKey1));
        assertThat(eventStore.lease(leaseKey2, ownerKey2, 2000).getOwnerKey(), is(ownerKey2));
    }

    @Test
    public void shouldLeaseMultipleKeysToOneOwner() {
        String leaseKey1 = randomKey("Lease1");
        String leaseKey2 = randomKey("Lease2");
        String ownerKey = randomKey("Owner");

        assertThat(eventStore.lease(leaseKey1, ownerKey, 2000).getOwnerKey(), is(ownerKey));
        assertThat(eventStore.lease(leaseKey2, ownerKey, 2000).getOwnerKey(), is(ownerKey));
    }

    @Test
    public void shouldLeaseToAnotherOwnerAfterExpiration() throws Exception {
        String leaseKey = randomKey("Lease");
        String ownerKey1 = randomKey("Owner1");
        String ownerKey2 = randomKey("Owner2");

        eventStore.lease(leaseKey, ownerKey1, 150);
        Thread.sleep(151);
        assertThat(eventStore.lease(leaseKey, ownerKey2, 2000).getOwnerKey(), is(ownerKey2));
    }

    @Test(expectedExceptions = EventStoreException.class)
    public void shouldRejectRenewalToNonOwner() throws Exception {
        String leaseKey = randomKey("Lease");
        String ownerKey = randomKey("Owner");

        eventStore.renewLease(leaseKey, ownerKey, 150);
    }

    @Test
    public void shouldRejectRenewalAfterExpirationIfTheOwnerHasChanged() throws Exception {
        String leaseKey = randomKey("Lease");
        String ownerKey1 = randomKey("Owner1");
        String ownerKey2 = randomKey("Owner2");

        eventStore.lease(leaseKey, ownerKey1, 150);
        Thread.sleep(151);
        eventStore.lease(leaseKey, ownerKey2, 150);
        Thread.sleep(151);
        assertThat(eventStore.renewLease(leaseKey, ownerKey1, 2000).getOwnerKey(), is(ownerKey2));
    }

    @Test
    public void shouldAllowRenewalAfterExpirationIfTheOwnerHasNotChanged() throws Exception {
        String leaseKey = randomKey("Lease");
        String ownerKey = randomKey("Owner1");

        eventStore.lease(leaseKey, ownerKey, 150);
        Thread.sleep(151);
        assertThat(eventStore.renewLease(leaseKey, ownerKey, 2000).getOwnerKey(), is(ownerKey));
    }

    @Test
    public void shouldGrantNewLeaseOnlyToOneConcurrentPretender() {
        String leaseKey = randomKey("Lease");

        Set<Lease> leases = IntStream.range(0, 50).parallel()
                .mapToObj(i -> eventStore.lease(leaseKey, randomKey("Owner" + i), 2000)).collect(toSet());
        assertThat("Expected exactly one lease in " + leases, leases, hasSize(1));
    }

    @Test
    public void shouldSaveNewEventsForCurrentLeaseOwner() throws Exception {
        String leaseKey = randomKey("Lease");
        String ownerKey = randomKey("Owner");

        Optional<Long> lastEvent = eventStore.getLastEventId();

        eventStore.lease(leaseKey, ownerKey, 2000);
        List<Long> newIds = eventStore.save(asList(textAppended("abc")), "Recipe", NEW_STREAM,
                asList(leaseKey), ownerKey);
        assertThat(newIds, hasSize(1));
        assertThat(eventStore.getLastEventId().get(), is(lastEvent.orElse(0L) + 1));
    }

    @Test
    public void shouldNotSaveNewEventsToNonLeaseOwner() {
        String leaseKey = randomKey("Lease");
        String ownerKey1 = randomKey("Owner1");
        String ownerKey2 = randomKey("Owner2");

        eventStore.lease(leaseKey, ownerKey1, 2000);

        Optional<Long> lastEvent = eventStore.getLastEventId();
        List<Long> newIds = eventStore.save(asList(textAppended("abc")), "Recipe", NEW_STREAM,
                asList(leaseKey), ownerKey2);
        assertThat(newIds, is(empty()));
        assertThat(eventStore.getLastEventId(), is(lastEvent));
    }

    @Test
    public void shouldNotSaveNewEventsToNonLeaseOwnerWhenLeaseNeverGranted() {
        String leaseKey = randomKey("Lease");
        String ownerKey = randomKey("Owner");

        Optional<Long> lastEvent = eventStore.getLastEventId();
        List<Long> newIds = eventStore.save(asList(textAppended("abc")), "Recipe", NEW_STREAM,
                asList(leaseKey), ownerKey);
        assertThat(newIds, is(empty()));
        assertThat(eventStore.getLastEventId(), is(lastEvent));
    }

    @Test
    public void shouldNotSaveNewEventsAfterExpiredLease() throws Exception {
        String leaseKey = randomKey("Lease");
        String ownerKey = randomKey("Owner");

        Optional<Long> lastEvent = eventStore.getLastEventId();

        eventStore.lease(leaseKey, ownerKey, 150);
        Thread.sleep(151);
        List<Long> newIds = eventStore.save(asList(textAppended("abc")), "Recipe", NEW_STREAM,
                asList(leaseKey), ownerKey);
        assertThat(newIds, is(empty()));
        assertThat(eventStore.getLastEventId(), is(lastEvent));
    }

    @Test
    public void shouldNotSaveNewEventsWhenNotAllLeasesOwned() throws Exception {
        String leaseKey1 = randomKey("Lease1");
        String leaseKey2 = randomKey("Lease2");
        String ownerKey1 = randomKey("Owner1");
        String ownerKey2 = randomKey("Owner2");

        Optional<Long> lastEvent = eventStore.getLastEventId();

        eventStore.lease(leaseKey1, ownerKey1, 2000);
        eventStore.lease(leaseKey2, ownerKey2, 2000);
        List<Long> newIds = eventStore.save(asList(textAppended("abc")), "Recipe", NEW_STREAM,
                asList(leaseKey1, leaseKey2), ownerKey1);
        assertThat(newIds, is(empty()));
        assertThat(eventStore.getLastEventId(), is(lastEvent));
    }

    private Event textAppended(String text) {
        return textAppended(streamId, text);
    }

    private Event textAppended(UUID streamId, String text) {
        return new Event(streamId, "TextAppended", randomUUID(), "{\"text\": \"" + text + "\"}");
    }

    private long save(Event event) {
        return save(event, NEW_STREAM);
    }

    private long save(Event event, long afterSequence) {
        List<Long> ids = save(asList(event), afterSequence);
        return ids.get(0);
    }

    private List<Long> save(List<Event> events) {
        return save(events, NEW_STREAM);
    }

    private List<Long> save(List<Event> events, long afterSequence) {
        return eventStore.save(events, "Recipe", afterSequence);
    }

    private List<Long> getIds(List<Event> events) {
        return events.stream().map(Event::getEventId).collect(toList());
    }

    private List<Long> getSequences(List<Event> events) {
        return events.stream().map(Event::getSequence).collect(toList());
    }

    private String randomKey(String prefix) {
        return String.format("%s-%05d", prefix, random.nextInt(100000));
    }

    static private Matcher<OffsetDateTime> after(final OffsetDateTime border) {
        return new TypeSafeMatcher<OffsetDateTime>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("Timestamp after " + border);
            }

            @Override
            protected boolean matchesSafely(OffsetDateTime item) {
                return item.isAfter(border);
            }
        };
    }

    static private <T> Matcher<List<T>> consistsOfConsistentBlocks(int blocks, int itemsInBlock) {
        return new TypeSafeMatcher<List<T>>() {
            @Override
            public void describeTo(Description description) {
                description.appendText(blocks + " consistent blocks of size " + itemsInBlock);
            }

            @Override
            protected boolean matchesSafely(List<T> item) {
                T value = null;
                for (int i = 0; i < 30; i++) {
                    if (i % 3 == 0) {
                        value = item.get(i);
                    } else {
                        if (!item.get(i).equals(value)) {
                            return false;
                        }
                    }
                }
                return true;
            }
        };

    }
}
