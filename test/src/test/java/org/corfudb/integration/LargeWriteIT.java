package org.corfudb.integration;

/** A set integration tests that exercise failure modes related to large writes. */
public class LargeWriteIT {

/**
    @Test
    public void test() {
        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();


        IStreamView sv = rt.getStreamsView().get(CorfuRuntime.getStreamID("stream1"));
        int numIter = 10_000;
        for (int x = 0; x < numIter; x++) {
            sv.append(new byte[1000]);
        }


    }
    **/
/**
    final Duration timeout = Duration.ofMinutes(5);
    final Duration pollPeriod = Duration.ofMillis(50);
    final int workflowNumRetry = 3;

    @Test
    public void form() {
        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();
        rt.getManagementView().addNode("localhost:9001", workflowNumRetry, timeout, pollPeriod);
        rt.getManagementView().addNode("localhost:9002", workflowNumRetry, timeout, pollPeriod);
    }

    @Test
    public void largeStreamWrite() throws Exception {

        CorfuRuntime rt1 = new CorfuRuntime("localhost:9000").connect();
        CorfuRuntime rt2 = new CorfuRuntime("localhost:9000").connect();

        RuntimeLayout rtl1 = rt1.getLayoutView().getRuntimeLayout();
        RuntimeLayout rtl2 = rt1.getLayoutView().getRuntimeLayout();


        Thread.sleep(1000 * 5);

        Runnable task =
                () -> {
                    AsyncChainReplicationProtocol repl = new AsyncChainReplicationProtocol();
                    byte[] payload = new byte[100];
                    int numWrites = 10000;
                    int burst = 20;
                    CompletableFuture<Boolean>[] cfs = new CompletableFuture[burst];
                    UUID streamId = CorfuRuntime.getStreamID("stream1");

                    long start = System.currentTimeMillis();
                    for (int x = 0; x < numWrites; x++) {

                        for (int bIdx = 0; bIdx < burst; bIdx++) {
                            cfs[bIdx] =
                                    rt1.getSequencerView()
                                            .nextAsync(streamId)
                                            .thenCompose(
                                                    (value) -> {
                                                        LogData ld = new LogData(DataType.DATA, payload);
                                                        ld.useToken(value);
                                                        return repl.writeAsync(rtl1, ld);
                                                    });
                        }
                        for (CompletableFuture<Boolean> cf : cfs) {
                            checkArgument(cf.join());
                        }
                    }

                    System.out.println(
                            (burst * 1.0 * numWrites) / (System.currentTimeMillis() * 1.0 - start));
                };

        Runnable task2 =
                () -> {
                    AsyncChainReplicationProtocol repl = new AsyncChainReplicationProtocol();
                    byte[] payload = new byte[100];
                    int numWrites = 10000;
                    int burst = 20;
                    CompletableFuture<Boolean>[] cfs = new CompletableFuture[burst];
                    UUID streamId = CorfuRuntime.getStreamID("stream1");

                    long start = System.currentTimeMillis();
                    for (int x = 0; x < numWrites; x++) {

                        for (int bIdx = 0; bIdx < burst; bIdx++) {
                            cfs[bIdx] =
                                    rt2.getSequencerView()
                                            .nextAsync(streamId)
                                            .thenCompose(
                                                    (value) -> {
                                                        LogData ld = new LogData(DataType.DATA, payload);
                                                        ld.useToken(value);
                                                        return repl.writeAsync(rtl2, ld);
                                                    });
                        }
                        for (CompletableFuture<Boolean> cf : cfs) {
                            checkArgument(cf.join());
                        }
                    }

                    System.out.println(
                            (burst * 1.0 * numWrites) / (System.currentTimeMillis() * 1.0 - start));
                };

        Executor executor = Executors.newFixedThreadPool(8);
        executor.execute(task);
        executor.execute(task);
        executor.execute(task);
        executor.execute(task2);
        executor.execute(task2);
        executor.execute(task2);

        ((ExecutorService) executor).shutdown();
        ((ExecutorService) executor).awaitTermination(10, TimeUnit.MINUTES);
    }
    **/
}
