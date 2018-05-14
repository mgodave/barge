package org.robotninjas.barge.state;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
*/
public class LogListener implements StateTransitionListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(RaftStateContext.class);

  @Override
  public void changeState(@Nonnull Raft context, @Nullable Raft.StateType from, @Nonnull Raft.StateType to) {
    LOGGER.info("LogListener: old state: {}, new state: {}", from, to);
  }

  @Override
  public void invalidTransition(@Nonnull Raft context, @Nonnull Raft.StateType actual, @Nullable Raft.StateType expected) {
    LOGGER.warn("LogListener: State transition from incorrect previous state.  Expected {}, was {}", actual, expected);
  }

  @Override
  public void stop(@Nonnull Raft raft) {
    LOGGER.info("Stopping {}", raft);
  }

}
