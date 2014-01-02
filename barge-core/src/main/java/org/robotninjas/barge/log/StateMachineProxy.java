package org.robotninjas.barge.log;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.*;
import com.google.inject.Inject;
import org.robotninjas.barge.StateMachine;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
class StateMachineProxy {

  private final int BATCH_SIZE = 10;

  private final ListeningExecutorService executor;
  private final StateMachine stateMachine;
  private final LinkedBlockingQueue<Runnable> operations;
  private final AtomicBoolean running;

  @Inject
  StateMachineProxy(@Nonnull @StateMachineExecutor ListeningExecutorService executor, @Nonnull StateMachine stateMachine) {
    this.executor = checkNotNull(executor);
    this.stateMachine = checkNotNull(stateMachine);
    this.operations = Queues.newLinkedBlockingQueue();
    this.running = new AtomicBoolean(false);
  }

  private void dispatch() {

    if (!running.get() && !operations.isEmpty()) {
      if (running.compareAndSet(false, true)) {

        final List<Runnable> ops = Lists.newArrayList();
        operations.drainTo(ops, BATCH_SIZE);

        executor.submit(new Runnable() {
          @Override
          public void run() {
            for (Runnable op : ops) {
              op.run();
            }
          }
        });
        running.set(false);
        dispatch();
      }
    }

  }

  private <V> ListenableFuture<V> submit(Callable<V> runnable) {

    ListenableFutureTask<V> operation =
        ListenableFutureTask.create(runnable);

    operations.offer(operation);
    dispatch();

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
