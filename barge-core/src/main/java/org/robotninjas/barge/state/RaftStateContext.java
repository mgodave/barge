/**
 * Copyright 2013 David Rusek <dave dot rusek at gmail dot com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.robotninjas.barge.state;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.RaftException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.robotninjas.barge.proto.RaftProto.*;

@NotThreadSafe
public class RaftStateContext {

  public enum StateType {START, FOLLOWER, CANDIDATE, LEADER}

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftStateContext.class);

  private final StateFactory stateFactory;
  private final Set<StateTransitionListener> listeners = Sets.newConcurrentHashSet();

  private volatile StateType state;
  private volatile State delegate;

  @Inject
  RaftStateContext(StateFactory stateFactory) {
    this.stateFactory = stateFactory;
    this.listeners.add(new LogListener());
  }

  @Nonnull
  public RequestVoteResponse requestVote(@Nonnull RequestVote request) {
    checkNotNull(request);
    return delegate.requestVote(this, request);
  }

  @Nonnull
  public AppendEntriesResponse appendEntries(@Nonnull AppendEntries request) {
    checkNotNull(request);
    return delegate.appendEntries(this, request);
  }

  @Nonnull
  public ListenableFuture<Object> commitOperation(@Nonnull byte[] op) throws RaftException {
    checkNotNull(op);
    return delegate.commitOperation(this, op);
  }

  public synchronized void setState(State oldState, @Nonnull StateType state) {
    if (this.delegate != oldState) {
      notifiesInvalidTransition(oldState);
      throw new IllegalStateException();
    }

    this.state = checkNotNull(state);
    switch (state) {
      case START:
        delegate = stateFactory.start();
        break;
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
    MDC.put("state", this.state.toString());

    notifiesChangeState(oldState);

    delegate.init(this);
  }

  private void notifiesInvalidTransition(State oldState) {
    for (StateTransitionListener listener : listeners) {
      listener.invalidTransition(this, state, oldState == null ? null : oldState.type());
    }
  }

  private void notifiesChangeState(State oldState) {
    for (StateTransitionListener listener : listeners) {
      listener.changeState(this, oldState == null ? null : oldState.type(), state);
    }
  }

  public void addTransitionListener(@Nonnull StateTransitionListener transitionListener) {
    listeners.add(transitionListener);
  }

  @Nonnull
  public StateType getState() {
    return state;
  }

  private class LogListener implements StateTransitionListener {
    @Override
    public void changeState(@Nonnull RaftStateContext context, @Nullable StateType from, @Nonnull StateType to) {
      LOGGER.info("old state: {}, new state: {}", from, to);
    }

    @Override
    public void invalidTransition(@Nonnull RaftStateContext context, @Nonnull StateType actual, @Nullable StateType expected) {
      LOGGER.warn("State transition from incorrect previous state.  Expected {}, was {}", actual, expected);
    }
  }
}
