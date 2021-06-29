package org.corfudb.runtime.view.replication;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.RuntimeLayout;

public class AsyncChainReplicationProtocol {

  /** Cancels the destination Future if the source Future is cancelled. */
  public static <X, Y> void propagateCancellation(
      ListenableFuture<? extends X> source,
      Future<? extends Y> destination,
      boolean mayInterruptIfRunning) {
    source.addListener(
        () -> {
          if (source.isCancelled()) {
            destination.cancel(mayInterruptIfRunning);
          }
        },
        directExecutor());
  }

  public static <V> ListenableFuture<V> toListenableFuture(CompletableFuture<V> completableFuture) {
    Objects.requireNonNull(completableFuture, "completableFuture is null");
    SettableFuture<V> future = SettableFuture.create();
    propagateCancellation(future, completableFuture, true);

    completableFuture.whenComplete(
        (value, exception) -> {
          if (exception != null) {
            future.setException(exception);
          } else {
            future.set(value);
          }
        });
    return future;
  }

  public AsyncChainReplicationProtocol() {}

  private void propagate(
      RuntimeLayout runtimeLayout,
      ILogData.SerializationHandle sh,
      int node,
      CompletableFuture<Boolean> promise) {

    if (node == runtimeLayout.getLayout().getAllLogServers().size()) {
      sh.close();
      promise.complete(true);
      return;
    }

    ListenableFuture<Boolean> chainWrite =
        toListenableFuture(
            runtimeLayout
                .getLogUnitClient(sh.getSerialized().getGlobalAddress(), node)
                .write(sh.getSerialized()));

    Futures.addCallback(
        chainWrite,
        new FutureCallback<Boolean>() {
          @Override
          public void onSuccess(@Nullable Boolean result) {
            checkArgument(result);
            propagate(runtimeLayout, sh, node + 1, promise);
          }

          @Override
          public void onFailure(Throwable t) {
            if (t instanceof OverwriteException) {
              // ignore
              propagate(runtimeLayout, sh, node + 1, promise);
              return;
            }
            sh.close();
            promise.completeExceptionally(t);
          }
        },
        directExecutor());
  }

  public CompletableFuture<Boolean> writeAsync(RuntimeLayout runtimeLayout, ILogData logData) {

    CompletableFuture<Boolean> promise = new CompletableFuture<>();

    ILogData.SerializationHandle sh = logData.getSerializedForm(true);

    CompletableFuture<Boolean> res =
        runtimeLayout
            .getLogUnitClient(sh.getSerialized().getGlobalAddress(), 0)
            .write(sh.getSerialized());

    ListenableFuture<Boolean> firstWrite = toListenableFuture(res);

    Futures.addCallback(
        firstWrite,
        new FutureCallback<Boolean>() {
          @Override
          public void onSuccess(@Nullable Boolean result) {
            checkArgument(result);
            propagate(runtimeLayout, sh, 1, promise);
          }

          @Override
          public void onFailure(Throwable t) {
            sh.close();
            promise.completeExceptionally(t);
          }
        },
        directExecutor());

    return promise;
  }
}
