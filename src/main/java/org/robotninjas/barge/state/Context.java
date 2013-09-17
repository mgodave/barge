package org.robotninjas.barge.state;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.inject.Inject;

import static org.robotninjas.barge.rpc.RaftProto.*;

public class Context {

  static enum StateType {FOLLOWER, CANDIDATE, LEADER};

  private static final Logger LOGGER = LoggerFactory.getLogger(Context.class);

  private final StateFactory stateFactory;
  private volatile StateType state;
  private volatile State delegate;

  @Inject
  Context(StateFactory stateFactory) {
    this.stateFactory = stateFactory;
    init();
  }

  public void init() {
    delegate = stateFactory.follower();
    state = StateType.FOLLOWER;
    delegate.init(this);
    MDC.put("state", state.toString());
  }

  public RequestVoteResponse requestVote(RequestVote request) {
    return delegate.requestVote(this, request);
  }

  public AppendEntriesResponse appendEntries(AppendEntries request) {
    return delegate.appendEntries(this, request);
  }

  void setState(StateType state) {
    MDC.put("state", state.toString());
    LOGGER.debug("old state: {}, new state: {}", this.state, state);
    this.state = state;
    switch (state) {
      case FOLLOWER:
        delegate = stateFactory.follower();
        break;
      case LEADER:
        delegate = stateFactory.leader();
        break;
      case CANDIDATE:
        delegate = stateFactory.candidate();
        break;
    }
    delegate.init(this);
  }

  @VisibleForTesting
  StateType getState() {
    return state;
  }

}
