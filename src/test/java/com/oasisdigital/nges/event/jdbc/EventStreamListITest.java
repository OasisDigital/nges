package com.oasisdigital.nges.event.jdbc;

import static java.time.OffsetDateTime.now;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.util.UUID;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.oasisdigital.nges.event.EventStoreConflict;
import com.oasisdigital.nges.event.EventStream;
import com.oasisdigital.nges.event.TestGroups;
import com.oasisdigital.nges.event.jdbc.ConnectionSource;
import com.oasisdigital.nges.event.jdbc.EventStreamListDao;

@Test(groups = TestGroups.INTEGRATION)
public class EventStreamListITest extends BaseITest {
    private ConnectionSource connectionSource;
    private EventStreamListDao streamList;
    private UUID id;

    @BeforeMethod
    public void setUp() throws Exception {
        this.connectionSource = new ConnectionSource(dataSource);
        this.streamList = new EventStreamListDao(connectionSource);
        this.id = UUID.randomUUID();
    }

    @Test
    public void shouldInitNewStream() throws Exception {
        initStreamIfNeeded(id, "StreamType");

        EventStream stream = streamList.findByStreamId(id);

        assertThat(stream.getStreamId(), is(id));
        assertThat(stream.getStreamType(), is("StreamType"));
        assertThat(stream.getLastEventId(), is(nullValue()));
        assertThat(stream.getLastSeqNo(), is(nullValue()));
        assertThat(stream.getLastTransactionTime(), is(nullValue()));
    }

    @Test
    public void shouldUpdateStream() throws Exception {
        initStreamIfNeeded(id, "StreamType");
        streamList.update(id, 112, 29, 0);

        EventStream stream = streamList.findByStreamId(id);

        assertThat(stream.getStreamId(), is(id));
        assertThat(stream.getStreamType(), is("StreamType"));
        assertThat(stream.getLastEventId(), is(112L));
        assertThat(stream.getLastSeqNo(), is(29L));
        assertThat(stream.getLastTransactionTime(), is(not(nullValue())));
        assertThat(stream.getLastTransactionTime().isAfter(now().minusSeconds(10)), is(true));
    }

    @Test
    public void initStreamIfNeeded_shouldDoNothingIfStreamAlreadyExists() throws Exception {
        initStreamIfNeeded(id, "StreamType");
        update(id, 112, 29, 0);
        initStreamIfNeeded(id, "StreamType");

        EventStream stream = streamList.findByStreamId(id);

        assertThat(stream.getStreamId(), is(id));
        assertThat(stream.getStreamType(), is("StreamType"));
        assertThat(stream.getLastEventId(), is(112L));
        assertThat(stream.getLastSeqNo(), is(29L));
        assertThat(stream.getLastTransactionTime(), is(not(nullValue())));
        assertThat(stream.getLastTransactionTime().isAfter(now().minusSeconds(10)), is(true));
    }

    @Test(expectedExceptions = EventStoreConflict.class)
    public void initStreamIfNeeded_shouldThrowConflictIfStreamExistsAndHasDifferentType() throws Exception {
        initStreamIfNeeded(id, "StreamType");
        initStreamIfNeeded(id, "AnotherType");
    }

    @Test(expectedExceptions = EventStoreConflict.class)
    public void initStream_shouldThrowConflictIfStreamExists() throws Exception {
        initStream(id, "StreamType");
        initStream(id, "StreamType");
    }

    @Test(expectedExceptions = EventStoreConflict.class)
    public void updateShouldThrowConflictIfTheIdInTableIsDifferentFromExpected() throws Exception {
        initStream(id, "StreamType");
        update(id, 100, 12, 0);
        update(id, 150, 20, 12);
        update(id, 160, 22, 12);
    }

    private void initStream(UUID streamId, String streamType) throws Exception {
        connectionSource.inTransaction(conn -> {
            streamList.initStream(streamId, streamType);
            return null;
        });
    }

    private boolean initStreamIfNeeded(UUID streamId, String streamType) throws Exception {
        return connectionSource.inTransaction(conn -> {
            return streamList.initStreamIfNeeded(streamId, streamType);
        });
    }

    private void update(UUID streamId, long lastEventId, long lastSequence, long expectedSequence)
            throws Exception {
        connectionSource.inTransaction(conn -> {
            streamList.update(streamId, lastEventId, lastSequence, expectedSequence);
            return null;
        });
    }
}
