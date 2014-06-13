package org.robotninjas.barge.jaxrs.ws;

import org.robotninjas.barge.state.Raft;
import org.robotninjas.barge.state.StateTransitionListener;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 */
public class WsEventListener implements StateTransitionListener {

  @Override
  public void changeState(@Nonnull Raft context, @Nullable Raft.StateType from, @Nonnull Raft.StateType to) {
  }

  @Override
  public void invalidTransition(@Nonnull Raft context, @Nonnull Raft.StateType actual, @Nullable Raft.StateType expected) {
  }

  @Override
  public void stop(@Nonnull Raft raft) {
  }
}
