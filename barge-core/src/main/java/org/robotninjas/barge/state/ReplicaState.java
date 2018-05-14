package org.robotninjas.barge.state;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.robotninjas.barge.api.AppendEntriesResponse;

public class ReplicaState {

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean ready = new AtomicBoolean(false);
  private volatile long nextIndex = 0;
  private volatile long matchIndex = 0;

  void init() {
    dispatch();
  }

  CompletableFuture<AppendEntriesResponse> sendUpdate() {
    return null;
  }

  void requestUpdate() {
    ready.set(true);
  }

  void dispatch() {

    if (!running.get() && ready.get()) {

      if (running.compareAndSet(false, true)) {

        ready.set(false);

        sendUpdate().handle((result, t) -> {
          if (null != t) {
            running.set(false);
          }
          return null;
        });

      }

    }

  }

}
