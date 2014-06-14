package org.robotninjas.barge.jaxrs.ws;

import com.google.common.collect.Sets;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.robotninjas.barge.state.Raft;
import org.robotninjas.barge.state.StateTransitionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;


/**
 */
public class WsEventListener implements StateTransitionListener {

  private final Logger logger = LoggerFactory.getLogger(WsEventListener.class);

  private Set<Listener> remotes = Sets.newConcurrentHashSet();
  private final Fiber executor;

  public WsEventListener() {
    this(new ThreadFiber(new RunnableExecutorImpl(), "ws-events", true));
  }

  public WsEventListener(Fiber fiber) {
    this.executor = fiber;
  }

  public void start() {
    executor.start();
  }

  public void stop() {
    executor.dispose();
  }

  @Override
  public void changeState(@Nonnull final Raft context, @Nullable final Raft.StateType from, @Nonnull final Raft.StateType to) {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        logger.debug("dispatching state changing: {}, {} -> {}", context, from, to);

        String message = "changing state of " + context + " " + from + " -> " + to;
        for (Listener remote : remotes) {
          remote.send(message);
        }
      }
    });
  }

  @Override
  public void invalidTransition(@Nonnull final Raft context, @Nonnull final Raft.StateType actual,
                                @Nullable final Raft.StateType expected) {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        logger.debug("dispatching invalid transition: {}, actual: {}, expected: {}", context, actual, expected);

        String message = "invalid transition in " + context + " expected: " + expected + ", actual: " + actual;
        for (Listener remote : remotes) {
          remote.send(message);
        }
      }
    });
  }

  @Override
  public void stop(@Nonnull final Raft raft) {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        logger.debug("dispatching stop: {}", raft);

        String message = "stopping " + raft;
        for (Listener remote : remotes) {
          remote.send(message);
        }
      }
    });
  }

  public void addClient(Listener listener) {
    logger.info("adding listener: {}", listener);

    remotes.add(listener);
  }


  public void removeClient(Listener listener) {
    logger.info("removing listener: {}", listener);

    remotes.remove(listener);
  }

  public void error(EventSocket socket, Throwable error) {
    logger.warn("caught error on socket {}", socket, error);
  }
}
