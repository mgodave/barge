package org.robotninjas.barge.state;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;
import org.jetlang.core.Disposable;
import org.jetlang.core.Scheduler;

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
    this.future = Optional.empty();
  }

  public void start() {
    checkState(!started);
    started = true;
    reset();
  }

  public void reset() {
    checkState(started);
    future.ifPresent(Disposable::dispose);
    future = Optional.of(scheduler.schedule(action, timeout, MILLISECONDS));
  }

  public void cancel() {
    checkState(started);
    future.ifPresent(Disposable::dispose);
  }

  public static DeadlineTimer start(Scheduler scheduler, Runnable action, long timeout) {
    DeadlineTimer t = new DeadlineTimer(scheduler, action, timeout);
    t.start();
    return t;
  }

}
