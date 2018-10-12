package org.corfudb.integration;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT  {

    @Test
    public void largeStreamWrite() throws Exception {
        final int numWriters = 32;
        final int numWrites = 32_000;
        int numClients = 1;
        String connString = "localhost:9000";
        final String tableName = "table1";
        final int sizeOfPayload = 140;
        final int batchSize = 100;

        CorfuRuntime[] runtimes = new CorfuRuntime[numClients];
        SMRMap<Long, byte[]>[] maps = new SMRMap[numClients];
        for (int x = 0; x < runtimes.length; x++) {
            runtimes[x] = new CorfuRuntime(connString).connect();
            maps[x] = runtimes[x].getObjectsView().build()
                    .setType(SMRMap.class)
                    .setStreamName(tableName)
                    .open();
        }


        ExecutorService executors = Executors.newFixedThreadPool(numWriters + 1);

        long startTime = System.currentTimeMillis();


        Future[] workersFutures = new Future[numWriters];
        for (int x = 0; x < numWriters; x++) {
            final int index = x % numClients;

            final long base  = (x * numWrites) + numWrites + 1000;


            workersFutures[x] = executors.submit(() -> {
                final byte[] payload = new byte[sizeOfPayload];

                int tempNumWrites = numWrites;
                for (int i = 0; tempNumWrites >= 0; i++) {

                    runtimes[index].getObjectsView().TXBuild()
                            .setType(TransactionType.WRITE_AFTER_WRITE).begin();
                    for (int j = 0; j < batchSize; j++) {
                        long key = i * batchSize + j + base;
                        maps[index].blindPut(key, payload);
                    }
                    runtimes[index].getObjectsView().TXEnd();
                    tempNumWrites = tempNumWrites - batchSize;
                }
                //System.out.println("Thread done " + Thread.currentThread().getId());
            });
        }


        for (Future future : workersFutures) {
            future.get();
            //System.out.println("Thread done");
        }

        long endTime = System.currentTimeMillis();
        System.out.println("write throughput (op/ms) " + ((numWriters * numWrites * 1.0) / (endTime - startTime)));

        startTime = System.currentTimeMillis();
        int numReads = 0;
        for (int x = 0; x < maps.length; x++) {
            numReads += maps[x].size();
        }
        endTime = System.currentTimeMillis();
        System.out.println("total read time " + (endTime - startTime)  + " number of entries read " + numReads);
    }
}
