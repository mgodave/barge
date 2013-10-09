package org.robotninjas.barge.log;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import org.robotninjas.barge.StateMachine;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

class StateMachineProxy {

  private final ListeningExecutorService executor;
  private final StateMachine stateMachine;

  @Inject
  public StateMachineProxy(@StateMachineExecutor @Nonnull ListeningExecutorService executor,
                           @Nonnull StateMachine stateMachine) {
    this.executor = executor;
    this.stateMachine = stateMachine;
  }

  public ListenableFuture<?> dispatchOperation(final ByteBuffer op) {
    return executor.submit(new Runnable() {
      @Override
      public void run() {
        stateMachine.applyOperation(op.asReadOnlyBuffer());
      }
    });
  }

}
