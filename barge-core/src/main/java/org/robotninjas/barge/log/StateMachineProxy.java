package org.robotninjas.barge.log;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import org.robotninjas.barge.StateMachine;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
class StateMachineProxy {

  private final ReentrantLock lock = new ReentrantLock();
  @GuardedBy("lock")
  private final ListeningExecutorService executor;
  private final StateMachine stateMachine;
  private final Supplier<File> snapshotFileSupplier;

  @Inject
  public StateMachineProxy(@StateMachineExecutor @Nonnull ListeningExecutorService executor,
                           @Nonnull StateMachine stateMachine, @Nonnull Supplier<File> snapshotFileSupplier) {
    this.executor = checkNotNull(executor);
    this.stateMachine = checkNotNull(stateMachine);
    this.snapshotFileSupplier = checkNotNull(snapshotFileSupplier);
  }

  @Nonnull
  public ListenableFuture<?> dispatchOperation(@Nonnull final ByteBuffer op) {

    checkNotNull(op);

    lock.lock();
    try {
      return executor.submit(new Runnable() {
        @Override
        public void run() {
          stateMachine.applyOperation(op.asReadOnlyBuffer());
        }
      });
    } finally {
      lock.unlock();
    }
  }

  @Nonnull
  public ListenableFuture<File> takeSnapshop() throws IOException {

    final File snaptshotFile = snapshotFileSupplier.get();
    final FileOutputStream fout = new FileOutputStream(snaptshotFile);

    lock.lock();
    try {
      return executor.submit(new Callable<File>() {
        @Override
        public File call() throws Exception {
//        stateMachine.takeSnapshot(fout);
          fout.close();
          return snaptshotFile;
        }
      });
    } finally {
      lock.unlock();
    }
  }

  @Nonnull
  public ListenableFuture<?> installSnapshot() {
    return Futures.immediateFailedFuture(new IllegalStateException());
  }

}
