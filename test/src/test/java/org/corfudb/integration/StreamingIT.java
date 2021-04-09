package org.corfudb.integration;


import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jdk.jfr.internal.JVM;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Simple test that inserts data into CorfuStore and tests Streaming.
 */
@Slf4j
public class StreamingIT {

    Map<String, String> globalMap = new ConcurrentHashMap<>(1000);

    private void createPool(CorfuTable<String, byte[]> pool, int numPartitions) {
        for (int x = 0; x < numPartitions; x++) {
            BitSet bitSet = new BitSet();
            pool.put("range_" + x, bitSet.toByteArray());
        }
    }

    @Test
    public void test() throws Exception {
        final int numRuntimes = 8;
        final int numPartitions = 8;
        final int partitionSize = 64 * 1000;
        final String poolName = "pool_1";
        final String connectionStr = "localhost:9000";
        int numAllocationsPerThread = 16000 * 2;

        final CorfuRuntime[] runtimes = new CorfuRuntime[numRuntimes];
        for (int x = 0; x < numRuntimes; x++) {
            runtimes[x] = new CorfuRuntime(connectionStr).connect();
        }

        // initialize pool
        final CorfuRuntime rt = runtimes[0];
        CorfuTable<String, byte[]> pool = rt.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, byte[]>>() {
                })
                .setStreamName(poolName)
                .open();

        rt.getObjectsView().TXBegin();
        createPool(pool, numPartitions);
        rt.getObjectsView().TXEnd();


        // Launch allocators
        ExecutorService allocatorRunners = Executors.newFixedThreadPool(numRuntimes,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("AllocationRunner-%d")
                        .build());

        for (int x = 0; x < numRuntimes; x++) {
            allocatorRunners.submit(new Allocator(x, partitionSize, poolName, numAllocationsPerThread, runtimes[x]));
        }

        allocatorRunners.shutdown();
        allocatorRunners.awaitTermination(5 * 4, TimeUnit.MINUTES);

        log.info("Allocated {} unique ids global map {}", globalMap.size(), globalMap);
    }

    class Allocator implements Runnable {

        private final int maxPartitionSize;
        private final int numAllocations;
        private final CorfuRuntime rt;
        private final ExecutorService executor;
        private final CorfuTable<String, byte[]> pool;
        private final int maxRetries = 50;

        public Allocator(int id, int maxPartitionSize, String poolName, int numAllocations, CorfuRuntime rt) {
            this.maxPartitionSize = maxPartitionSize;
            this.numAllocations = numAllocations;
            this.rt = rt;
            this.pool = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, byte[]>>() {
                    })
                    .setStreamName(poolName)
                    .open();

            this.executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                    .setNameFormat("allocator-thread-" + id)
                    .build());
        }

        @Override
        public void run() {
            for (int x = 0; x < numAllocations; x++) {
                try {
                    rt.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
                    CompletableFuture<String> allocation = makeAllocation();
                    String allocatedId = allocation.join();
                    String old = globalMap.put(allocatedId, "");
                    if (old != null) {
                        log.error("Duplicate id detected {} map is {} ", old, globalMap);
                        System.exit(-3);
                    }
                    // Add this somewhere?
                    rt.getObjectsView().TXEnd();
                } catch (Exception e) {
                    log.warn("Failed to allocate", e);
                }
            }
        }

        CompletableFuture<String> makeAllocation() {
            return CompletableFuture.supplyAsync(() -> {

                for (int x = 0; x < maxRetries; x++) {
                    try {
                        rt.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();

                        List<Map.Entry<String, byte[]>> allPartitions = new ArrayList<>(pool.entrySet());
                        List<Map.Entry<String, byte[]>> freePartitions = new ArrayList<>();

                        for (Map.Entry<String, byte[]> currentPartition : allPartitions) {
                            BitSet bitSet = BitSet.valueOf(currentPartition.getValue());
                            if (bitSet.cardinality() == bitSet.size() && bitSet.size() == maxPartitionSize) {
                                continue;
                            }
                            freePartitions.add(currentPartition);
                        }

                        if (freePartitions.isEmpty()) {
                            rt.getObjectsView().TXEnd();
                            throw new IllegalStateException("no available Ids!");
                        }

                        int randIdx = ThreadLocalRandom.current().nextInt(freePartitions.size());
                        Map.Entry<String, byte[]> selectedPartition = freePartitions.get(randIdx);
                        BitSet bitSet = BitSet.valueOf(selectedPartition.getValue());
                        int nextAvailableId = bitSet.nextClearBit(0);
                        bitSet.set(nextAvailableId);
                        pool.put(selectedPartition.getKey(), bitSet.toByteArray());
                        rt.getObjectsView().TXEnd();
                        return selectedPartition.getKey() + "_" + nextAvailableId;
                    } catch (TransactionAbortedException tae) {
                        try {
                            Thread.sleep(ThreadLocalRandom.current().nextInt(5));
                        } catch (Exception e) {
                            log.error("sleep error", e);
                        }
                        // concurrent exception!, need to try again
                        if (tae.getAbortCause() != AbortCause.CONFLICT) {
                            log.warn("Failed allocation on attempt {} cause {}", x, tae.getAbortCause());
                        }
                        if (x > maxRetries / 2) {
                            log.warn("Failed allocation on attempt {} cause {}", x, tae.getAbortCause());
                        }
                    }
                }
                throw new IllegalStateException("no available Ids2!");
            }, executor);
        }
    }
}
