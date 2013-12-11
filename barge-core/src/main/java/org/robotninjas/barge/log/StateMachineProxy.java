package org.robotninjas.barge.log;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import org.robotninjas.barge.StateMachine;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

@ThreadSafe
class StateMachineProxy {

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
  public ListenableFuture dispatchOperation(final long index, @Nonnull final ByteBuffer op, final SettableFuture<Object> listener) {

    checkNotNull(op);

    return executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          Object result = stateMachine.applyOperation(op.asReadOnlyBuffer());
          if (listener != null) {
            listener.set(result);
          }
        } catch (Throwable t) {
          if (listener != null) {
            listener.setException(t);
          }
        }
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

}
