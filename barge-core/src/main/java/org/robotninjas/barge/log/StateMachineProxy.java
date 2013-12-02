package org.robotninjas.barge.log;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import org.robotninjas.barge.LogAware;
import org.robotninjas.barge.StateMachine;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

@ThreadSafe
class StateMachineProxy implements LogAware {

  private final ListeningExecutorService executor;
  private final StateMachine stateMachine;

  @VisibleForTesting
  StateMachineProxy(@Nonnull ListeningExecutorService executor, @Nonnull StateMachine stateMachine) {
    this.executor = checkNotNull(executor);
    this.stateMachine = checkNotNull(stateMachine);
  }

  @Inject
  StateMachineProxy(@Nonnull StateMachine stateMachine) {
    this(listeningDecorator(newSingleThreadExecutor()), stateMachine);
  }

  @Nonnull
  public ListenableFuture dispatchOperation(@Nonnull final ByteBuffer op) {

    checkNotNull(op);

    return executor.submit(new Runnable() {
      @Override
      public void run() {
        stateMachine.applyOperation(op.asReadOnlyBuffer());
      }
    });

  }

  @Nonnull
  public ListenableFuture takeSnapshot(@Nonnull final OutputStream out) throws IOException {

    return executor.submit(new Runnable() {
      @Override
      public void run() {
        //stateMachine.takeSnapshot(out);
      }
    });
  }

  @Nonnull
  public ListenableFuture installSnapshot() {
    return Futures.immediateFailedFuture(new IllegalStateException());
  }

  @Override
  public void setLog(RaftLog log) {
    if (stateMachine instanceof LogAware) {
      ((LogAware) stateMachine).setLog(log);
    }
  }

}
