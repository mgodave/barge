package org.robotninjas.barge.log;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Runnables.doNothing;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Runnables;
import com.google.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.robotninjas.barge.StateMachine;

@ThreadSafe
class StateMachineProxy {

  private final int BATCH_SIZE = 10;

  private final Executor executor;
  private final StateMachine stateMachine;
  private final LinkedBlockingQueue<Runnable> operations;
  private final AtomicBoolean running;

  @Inject
  StateMachineProxy(@Nonnull @StateExecutor Executor executor, @Nonnull StateMachine stateMachine) {
    this.executor = checkNotNull(executor);
    this.stateMachine = checkNotNull(stateMachine);
    this.operations = Queues.newLinkedBlockingQueue();
    this.running = new AtomicBoolean(false);
  }

  private <V> CompletableFuture<V> submit(Callable<V> runnable) {
    return CompletableFuture.runAsync(doNothing(), executor).thenApply((ignored) -> {
      try {
        return runnable.call();
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Nonnull
  public CompletableFuture<Object> dispatchOperation(@Nonnull final ByteBuffer op) {
    checkNotNull(op);
    return submit(() -> stateMachine.applyOperation(op.asReadOnlyBuffer()));
  }

  @Nonnull
  public CompletableFuture takeSnapshot(@Nonnull final OutputStream out) throws IOException {
    checkNotNull(out);
    return submit((Callable) () -> {
      //stateMachine.takeSnapshot(out);
      return null;
    });
  }

  @Nonnull
  public CompletableFuture installSnapshot() {
    CompletableFuture failed = new CompletableFuture();
    failed.completeExceptionally(new IllegalStateException());
    return failed;
  }

}
