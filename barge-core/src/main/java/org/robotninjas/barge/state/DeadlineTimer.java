package org.robotninjas.barge.state;

import com.google.common.base.Optional;
import org.jetlang.core.Disposable;
import org.jetlang.core.Scheduler;

import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@NotThreadSafe
class DeadlineTimer {

  private final Scheduler scheduler;
  private final Runnable action;
  private final long timeout;
  private boolean started = false;
  private Optional<Disposable> future;

  DeadlineTimer(Scheduler scheduler, Runnable action, long timeout) {
    this.scheduler = scheduler;
    this.action = action;
    this.timeout = timeout;
    this.future = Optional.absent();
  }

  public void start() {
    checkState(!started);
    started = true;
    reset();
  }

  public void reset() {
    checkState(started);
    if (future.isPresent()) {
      future.get().dispose();
    }
    future = Optional.of(scheduler.schedule(action, timeout, MILLISECONDS));
  }

  public void cancel() {
    checkState(started);
    if (future.isPresent()) {
      future.get().dispose();
    }
  }

  public static DeadlineTimer start(Scheduler scheduler, Runnable action, long timeout) {
    DeadlineTimer t = new DeadlineTimer(scheduler, action, timeout);
    t.start();
    return t;
  }

}
