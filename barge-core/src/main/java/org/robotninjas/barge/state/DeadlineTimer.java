package org.robotninjas.barge.state;

import com.google.common.base.Optional;

import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@NotThreadSafe
class DeadlineTimer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeadlineTimer.class);

  private final ScheduledExecutorService scheduler;
  private final Runnable action;
  private final long timeout;
  private boolean started = false;
  private Optional<? extends ScheduledFuture<?>> future;

  DeadlineTimer(ScheduledExecutorService scheduler, Runnable action, long timeout) {
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
      boolean cancelled = future.get().cancel(false);
      if (!cancelled) {
        LOGGER.error("Unable to cancel execution of task");
      }
    }
    future = Optional.of(scheduler.schedule(action, timeout, MILLISECONDS));
  }

  public void cancel() {
    checkState(started);
    if (future.isPresent()) {
      boolean cancelled = future.get().cancel(false);
      if (!cancelled) {
        LOGGER.error("Unable to cancel execution of task");
      }
    }
  }

  public static DeadlineTimer start(ScheduledExecutorService scheduler, Runnable action, long timeout) {
    DeadlineTimer t = new DeadlineTimer(scheduler, action, timeout);
    t.start();
    return t;
  }

}
