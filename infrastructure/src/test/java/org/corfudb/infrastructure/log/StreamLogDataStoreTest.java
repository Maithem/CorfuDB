package org.corfudb.infrastructure.log;

import static org.junit.Assert.assertEquals;


import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.datastore.DataStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@Slf4j
public class StreamLogDataStoreTest {
    private static final long ZERO_ADDRESS = 0L;
    private static final long NON_ADDRESS = -1;

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testGetAndSave() {
        StreamLogDataStore streamLogDs = getStreamLogDataStore();
        final long tailSegment = 333;
        final long startingAddress = 444;
        final long committedTail = 555;

        streamLogDs.updateStartingAddress(startingAddress);
        streamLogDs.updateCommittedTail(committedTail);

        assertEquals(startingAddress, streamLogDs.getStartingAddress());
        assertEquals(committedTail, streamLogDs.getCommittedTail());

        // StartingAddress should be monotonic.
        streamLogDs.updateStartingAddress(startingAddress - 1);
        assertEquals(startingAddress, streamLogDs.getStartingAddress());
        streamLogDs.updateStartingAddress(startingAddress + 1);
        assertEquals(startingAddress + 1, streamLogDs.getStartingAddress());

        // CommittedTail should be monotonic.
        streamLogDs.updateCommittedTail(committedTail - 1);
        assertEquals(committedTail, streamLogDs.getCommittedTail());
        streamLogDs.updateCommittedTail(committedTail + 1);
        assertEquals(committedTail + 1, streamLogDs.getCommittedTail());
    }

    @Test
    public void testReset() {
        StreamLogDataStore streamLogDs = getStreamLogDataStore();
        streamLogDs.resetStartingAddress();
        assertEquals(ZERO_ADDRESS, streamLogDs.getStartingAddress());

        streamLogDs.resetCommittedTail();
        assertEquals(NON_ADDRESS, streamLogDs.getCommittedTail());
    }

    private StreamLogDataStore getStreamLogDataStore() {
        Map<String, Object> opts = new HashMap<>();
        opts.put("--log-path", tempDir.getRoot().getAbsolutePath());

        DataStore ds = new DataStore(opts, val -> log.info("clean up"));

        return new StreamLogDataStore(ds);
    }
}
