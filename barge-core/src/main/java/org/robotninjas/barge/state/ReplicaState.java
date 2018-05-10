package org.robotninjas.barge.state;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.robotninjas.barge.api.AppendEntriesResponse;

public class ReplicaState {

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean ready = new AtomicBoolean(false);
  private volatile long nextIndex = 0;
  private volatile long matchIndex = 0;

  void init() {
    dispatch();
  }

  ListenableFuture<AppendEntriesResponse> sendUpdate() {
    return null;
  }

  void requestUpdate() {
    ready.set(true);
  }

  void dispatch() {

    if (!running.get() && ready.get()) {

      if (running.compareAndSet(false, true)) {

        ready.set(false);

        Futures.addCallback(sendUpdate(), new FutureCallback<AppendEntriesResponse>() {

          @Override
          public void onSuccess(@Nullable AppendEntriesResponse result) {
            if (result.getSuccess()) {

            } else {

            }
          }

          @Override
          public void onFailure(Throwable t) {
            running.set(false);
          }

        }, directExecutor());

      }

    }

  }

}
