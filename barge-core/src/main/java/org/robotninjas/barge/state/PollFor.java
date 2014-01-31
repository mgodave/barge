package org.robotninjas.barge.state;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public abstract class PollFor<T> {
  private static final Logger log = LoggerFactory.getLogger(PollFor.class);

  protected final SettableFuture<T> future;
  final ScheduledExecutorService executor;
  final int initial;
  final int interval;
  final TimeUnit timeUnit;

  protected abstract Optional<T> poll();

  public PollFor(ScheduledExecutorService executor, int initial, int interval, TimeUnit timeUnit) {
    this.executor = executor;
    this.initial = initial;
    this.interval = interval;
    this.timeUnit = timeUnit;
    this.future = SettableFuture.create();
  }

  public ListenableFuture<T> start() {
    executor.schedule(new Runnable() {

      @Override
      public void run() {
        doPoll();
      }
    }, initial, timeUnit);
    return future;
  }

  protected final void doPoll() {
    try {
      doPoll0();
    } catch (Throwable t) {
      log.error("Error while polling", t);
    }
  }

  protected void doPoll0() {
    if (future.isCancelled()) {
      return;
    }

    try {
      Optional<T> result = poll();
      if (result.isPresent()) {
        future.set(result.get());
      }
    } catch (Throwable t) {
      future.setException(t);
    }

    if (!future.isDone()) {
      executor.schedule(new Runnable() {

        @Override
        public void run() {
          doPoll();
        }

      }, interval, timeUnit);
    }

  }
}
