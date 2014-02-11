package org.robotninjas.barge.log;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.inject.Inject;
import org.robotninjas.barge.StateMachine;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

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

  private <V> ListenableFuture<V> submit(Callable<V> runnable) {

    ListenableFutureTask<V> operation =
        ListenableFutureTask.create(runnable);

    executor.execute(operation);

    return operation;

  }

  @Nonnull
  public ListenableFuture<Object> dispatchOperation(@Nonnull final ByteBuffer op) {
    checkNotNull(op);
    return submit(new Callable<Object>() {
      @Override
      public Object call() {
        return stateMachine.applyOperation(op.asReadOnlyBuffer());
      }
    });
  }

  @Nonnull
  public ListenableFuture takeSnapshot(@Nonnull final OutputStream out) throws IOException {
    checkNotNull(out);
    return submit(new Callable() {
      @Override
      public Object call() {
        //stateMachine.takeSnapshot(out);
        return null;
      }
    });
  }

  @Nonnull
  public ListenableFuture installSnapshot() {
    return Futures.immediateFailedFuture(new IllegalStateException());
  }

}
